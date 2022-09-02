---
toc_folder_title: "\u8BBE\u7F6E"
---

# 设置 {#settings}

有多种方法可以进行以下所述的所有设置。
设置是在图层中配置的，因此每个后续图层都会重新定义以前的设置。

按优先级顺序配置设置的方法:

-   在设置 `users.xml` 服务器配置文件。

        Set in the element `<profiles>`.

-   会话设置。

        Send ` SET setting=value` from the ClickHouse console client in interactive mode.

    同样，您可以在HTTP协议中使用ClickHouse会话。 要做到这一点，你需要指定 `session_id` HTTP参数。

-   查询设置。

    -   在非交互模式下启动ClickHouse控制台客户端时，设置startup参数 `--setting=value`.
    -   使用HTTP API时，请传递CGI参数 (`URL?setting_1=value&setting_2=value...`).

本节不介绍只能在服务器配置文件中进行的设置。

[原始文章](https://clickhouse.com/docs/en/operations/settings/) <!--hide-->
