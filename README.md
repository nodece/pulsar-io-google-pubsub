## Google Cloud Pub/Sub connector

The [Google Cloud Pub/Sub](https://cloud.google.com/pubsub) connector is a [Pulsar IO connector](http://pulsar.apache.org/docs/en/next/io-overview/) for copying data between Google Cloud Pub/Sub and Pulsar. It contains two types of connectors:

- Google Cloud Pub/Sub source connector


    This connector feeds data from Google Cloud Pub/Sub and writes data to Pulsar topics. 

    ![](docs/google-pubsub-source.png)

- Google Cloud Pub/Sub sink connector

    This connector pulls data from Pulsar topics and persists data to Google Cloud Pub/Sub.

    ![](docs/google-pubsub-sink.png)

Currently, Google Cloud Pub/Sub connector versions (`x.y.z`) are based on Pulsar versions (`x.y.z`).

| Google Cloud Pub/Sub connector version | Pulsar version | Doc |
| :---------- | :------------------- | :------------- |
[2.8.x](https://github.com/streamnative/pulsar-io-google-pubsub/releases/tag/v2.8.2.0)| [2.8.0](http://pulsar.apache.org/en/download/) | - [Google Cloud Pub/Sub source connector](https://hub.streamnative.io/connectors/google-pubsub-source/v2.8.1.29/)<br><br>- [Google Cloud Pub/Sub sink connector](https://hub.streamnative.io/connectors/google-pubsub-sink/v2.8.1.29/) |
[2.9.x](https://github.com/streamnative/pulsar-io-google-pubsub/releases/tag/v2.9.1.0-rc2)| [2.9.0](http://pulsar.apache.org/en/download/) | - [Google Cloud Pub/Sub source connector](https://hub.streamnative.io/connectors/google-pubsub-source/v2.9.1.1/)<br><br>- [Google Cloud Pub/Sub sink connector](https://hub.streamnative.io/connectors/google-pubsub-sink/v2.9.1.1/) |

## Project layout

Below are the sub folders and files of this project and their corresponding descriptions.

```bash
├── conf // examples of configuration files of this connector
├── docs // user guides of this connector
├── script // scripts of this connector
├── src // source code of this connector
│   ├── checkstyle // checkstyle configuration files of this connector
│   ├── license // license header for this project. `mvn license:format` can
    be used for formatting the project with the stored license header in this directory
│   │   └── ALv2
│   ├── main // main source files of this connector
│   │   └── java
│   ├── spotbugs // spotbugs configuration files of this connector
│   └── test // test related files of this connector
│       └── java
```

## License

Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0