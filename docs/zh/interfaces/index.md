---
sidebar_label: 接口
sidebar_position: 14
---

# 客户端 {#interfaces}

ClickHouse提供了两个网络接口(两个都可以选择包装在TLS中以增加安全性):

-   [HTTP](http.md), 包含文档，易于使用。
-   [Native TCP](../interfaces/tcp.md)，简单，方便使用。

在大多数情况下，建议使用适当的工具或库，而不是直接与它们交互。Yandex官方支持的项目有:

-   [命令行客户端](../interfaces/cli.md)
-   [JDBC驱动](../interfaces/jdbc.md)
-   [ODBC驱动](../interfaces/odbc.md)
-   [C++客户端](../interfaces/cpp.md)

还有一些广泛的第三方库可供ClickHouse使用:

-   [客户端库](../interfaces/third-party/client-libraries.md)
-   [第三方集成库](../interfaces/third-party/integrations.md)
-   [可视化UI](../interfaces/third-party/gui.md)

[来源文章](https://clickhouse.com/docs/en/interfaces/) <!--hide-->
