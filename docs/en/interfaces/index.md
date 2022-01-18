---
toc_folder_title: Interfaces
toc_priority: 14
toc_title: Introduction
---

# Interfaces {#interfaces}

ClickHouse provides two network interfaces (both can be optionally wrapped in TLS for additional security):

-   [HTTP](http.md), which is documented and easy to use directly.
-   [Native TCP](../interfaces/tcp.md), which has less overhead.

In most cases it is recommended to use appropriate tool or library instead of interacting with those directly. Officially supported by Yandex are the following:

-   [Command-line client](../interfaces/cli.md)
-   [JDBC driver](../interfaces/jdbc.md)
-   [ODBC driver](../interfaces/odbc.md)
-   [C++ client library](../interfaces/cpp.md)

There are also a wide range of third-party libraries for working with ClickHouse:

-   [Client libraries](../interfaces/third-party/client-libraries.md)
-   [Integrations](../interfaces/third-party/integrations.md)
-   [Visual interfaces](../interfaces/third-party/gui.md)

[Original article](https://clickhouse.tech/docs/en/interfaces/) <!--hide-->
