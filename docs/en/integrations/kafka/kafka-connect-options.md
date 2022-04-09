---
sidebar_label: Kafka Connect Options
sidebar_position: 5
description: Options with Kafka Connect
---

# Connection Options

Kafka Connect uses Sink Connectors to deliver data from Kafka topics into other data stores such as ClickHouse. Two Sink connectors provided by Confluent are compatible with ClickHouse:

* [JDBC Connector](https://docs.confluent.io/kafka-connect-jdbc/current/) - This Connector is both a Sink and Source Connector (for pushing data to Kafka) via the JDBC interface.
* [HTTP Sink Connector](https://docs.confluent.io/kafka-connect-http/current/overview.html) - A connector for pulling data from Kafka and inserting it via its HTTP interface.

**Limitations**

Each of these has benefits and limitations:


* The [JDBC connector](./kafka-connect-jdbc) relies on the user providing a [JDBC driver](https://github.com/ClickHouse/clickhouse-jdbc). This driver has several versions, including the official ClickHouse distribution. This version uses the HTTP interface, although native support is planned. Until the native interface is not supported, it provides no performance benefit over the HTTP Sink other than ease of configuration. [Other drivers](https://github.com/housepower/ClickHouse-Native-JDBC) support the native protocol, but these have not been tested.
* The JDBC connector requires a Kafka schema defining the types of the fields. It uses this schema, defined in JSON schema, to formulate insert statements. Whilst this is effective on primitive types, the connector does not support ClickHouse specific types, e.g., Arrays and Maps. Furthermore, this connector will not support several configuration options which rely on DDL queries - highlighted in the section [JDBC Connector](./kafka-connect-jdbc) below.
* The [HTTP Sink Connector](./kafka-connect-http) does not require a data schema. Our example assumes the data is in JSON format - although this approach should be compatible with any [formats](https://clickhouse.com/docs/en/interfaces/formats/#data-formatting) that the ClickHouse HTTP interface can consume. 
* The HTTP Sink Connector is also deployed natively in Confluent Cloud and has been tested with ClickHouse Cloud, unlike the JDBC, which must be self-managed. We provide instructions for both scenarios below.
* The JDBC connector is not currently hosted in Confluent Cloud. This must be self-managed.
* Both connectors have at-least-once delivery semantics. Duplicates may therefore occur in ClickHouse. 

The JDBC Connector is distributed under the [Confluent Community License](https://www.confluent.io/confluent-community-license). The HTTP Connector conversely requires a [Confluent Enterprise License](https://docs.confluent.io/kafka-connect-http/current/overview.html#license).
