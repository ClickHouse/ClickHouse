---
sidebar_label: Kafka Connect
sidebar_position: 4
description: Introduction to Kafka Connect
---

# Using Kafka Connect

For users that prefer to manage the Kafka ClickHouse interaction external to ClickHouse, Kafka Connect provides an alternative. As of the time of writing, there are several approaches to using Kafka Connect, depending on the direction of data transfer, each with its own limitations. The following is not a comprehensive tutorial on Kafka Connect, and the user is referred to Confluent documentation for advanced configurations.

### Pre-requisites


1. Download and install the Confluent platform [[https://www.confluent.io/installation](https://www.confluent.io/installation)](https://www.confluent.io/installation](https://www.confluent.io/installation)). This main Confluent package contains the tested version of Kafka Connect v7.0.1. 
2. Java is required for the Confluent Platform. Refer to their documentation for the currently [supported java versions](https://docs.confluent.io/platform/current/installation/versions-interoperability.html).
3. Ensure you have a ClickHouse instance available.
4. Kafka instance - Confluent cloud is the easiest for this; otherwise, set up a self-managed instance using the above Confluent package. The setup of Kafka is beyond the scope of these docs.
5. Test dataset. A small GitHub JSON-based dataset with an insertion script is provided for convenience [here](https://github.com/ClickHouse/kafka-samples/tree/main/producer). This will automatically apply a Kafka schema to the data to ensure it is compatible with the JDBC connector. 

### Assumptions

* We assume you are familiar with the Confluent Platform, specifically Kafka Connect. We recommend the [Getting Started guide](https://docs.confluent.io/platform/current/connect/userguide.html) for Kafka Connect and the [Kafka Connect 101](https://developer.confluent.io/learn-kafka/kafka-connect) guide.
* We assume your source data is in JSON. Other data formats and the relevant configuration parameters are highlighted.
