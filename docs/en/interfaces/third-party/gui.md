---
toc_priority: 28
toc_title: Visual Interfaces
---

# Visual Interfaces from Third-party Developers {#visual-interfaces-from-third-party-developers}

## Open-Source {#open-source}

### Tabix {#tabix}

Web interface for ClickHouse in the [Tabix](https://github.com/tabixio/tabix) project.

Features:

-   Works with ClickHouse directly from the browser, without the need to install additional software.
-   Query editor with syntax highlighting.
-   Auto-completion of commands.
-   Tools for graphical analysis of query execution.
-   Colour scheme options.

[Tabix documentation](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) is a UI/IDE for OSX, Linux and Windows.

Features:

-   Query builder with syntax highlighting. View the response in a table or JSON view.
-   Export query results as CSV or JSON.
-   List of processes with descriptions. Write mode. Ability to stop (`KILL`) a process.
-   Database graph. Shows all tables and their columns with additional information.
-   A quick view of the column size.
-   Server configuration.

The following features are planned for development:

-   Database management.
-   User management.
-   Real-time data analysis.
-   Cluster monitoring.
-   Cluster management.
-   Monitoring replicated and Kafka tables.

### LightHouse {#lighthouse}

[LightHouse](https://github.com/VKCOM/lighthouse) is a lightweight web interface for ClickHouse.

Features:

-   Table list with filtering and metadata.
-   Table preview with filtering and sorting.
-   Read-only queries execution.

### Redash {#redash}

[Redash](https://github.com/getredash/redash) is a platform for data visualization.

Supports for multiple data sources including ClickHouse, Redash can join results of queries from different data sources into one final dataset.

Features:

-   Powerful editor of queries.
-   Database explorer.
-   Visualization tools, that allow you to represent data in different forms.

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - universal desktop database client with ClickHouse support.

Features:

-   Query development with syntax highlight and autocompletion.
-   Table list with filters and metadata search.
-   Table data preview.
-   Full-text search.

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) is an alternative command-line client for ClickHouse, written in Python 3.

Features:

-   Autocompletion.
-   Syntax highlighting for the queries and data output.
-   Pager support for the data output.
-   Custom PostgreSQL-like commands.

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) is a specialized tool to visualize the `system.trace_log` as [flamegraph](http://www.brendangregg.com/flamegraphs.html).

### clickhouse-plantuml {#clickhouse-plantuml}

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) is a script to generate [PlantUML](https://plantuml.com/) diagram of tables’ schemes.

### xeus-clickhouse {#xeus-clickhouse}

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) is a Jupyter kernal for ClickHouse, which supports query CH data using SQL in Jupyter.

## Commercial {#commercial}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) is a database IDE from JetBrains with dedicated support for ClickHouse. It is also embedded in other IntelliJ-based tools: PyCharm, IntelliJ IDEA, GoLand, PhpStorm and others.

Features:

-   Very fast code completion.
-   ClickHouse syntax highlighting.
-   Support for features specific to ClickHouse, for example, nested columns, table engines.
-   Data Editor.
-   Refactorings.
-   Search and Navigation.

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens) is a service of data visualization and analytics.

Features:

-   Wide range of available visualizations, from simple bar charts to complex dashboards.
-   Dashboards could be made publicly available.
-   Support for multiple data sources including ClickHouse.
-   Storage for materialized data based on ClickHouse.

DataLens is [available for free](https://cloud.yandex.com/docs/datalens/pricing) for low-load projects, even for commercial use.

-   [DataLens documentation](https://cloud.yandex.com/docs/datalens/).
-   [Tutorial](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) on visualizing data from a ClickHouse database.

### Holistics Software {#holistics-software}

[Holistics](https://www.holistics.io/) is a full-stack data platform and business intelligence tool.

Features:

-   Automated email, Slack and Google Sheet schedules of reports.
-   SQL editor with visualizations, version control, auto-completion, reusable query components and dynamic filters.
-   Embedded analytics of reports and dashboards via iframe.
-   Data preparation and ETL capabilities.
-   SQL data modelling support for relational mapping of data.

### Looker {#looker}

[Looker](https://looker.com) is a data platform and business intelligence tool with support for 50+ database dialects including ClickHouse. Looker is available as a SaaS platform and self-hosted. Users can use Looker via the browser to explore data, build visualizations and dashboards, schedule reports, and share their insights with colleagues. Looker provides a rich set of tools to embed these features in other applications, and an API
to integrate data with other applications.

Features:

-   Easy and agile development using LookML, a language which supports curated
    [Data Modeling](https://looker.com/platform/data-modeling) to support report writers and end-users.
-   Powerful workflow integration via Looker’s [Data Actions](https://looker.com/platform/actions).

[How to configure ClickHouse in Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[Original article](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
