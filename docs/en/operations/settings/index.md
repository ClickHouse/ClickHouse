---
toc_folder_title: Settings
toc_priority: 55
toc_title: Introduction
---

# Settings {#session-settings-intro}

There are multiple ways to make all the settings described in this section of documentation.

Settings are configured in layers, so each subsequent layer redefines the previous settings.

Ways to configure settings, in order of priority:

-   Settings in the `users.xml` server configuration file.

    Set in the element `<profiles>`.

-   Session settings.

    Send `SET setting=value` from the ClickHouse console client in interactive mode.
    Similarly, you can use ClickHouse sessions in the HTTP protocol. To do this, you need to specify the `session_id` HTTP parameter.

-   Query settings.

    -   When starting the ClickHouse console client in non-interactive mode, set the startup parameter `--setting=value`.
    -   When using the HTTP API, pass CGI parameters (`URL?setting_1=value&setting_2=value...`).

Settings that can only be made in the server config file are not covered in this section.

[Original article](https://clickhouse.tech/docs/en/operations/settings/) <!--hide-->
