# Visual Interfaces from Third-party Developers

## Open-Source

### Tabix

Web interface for ClickHouse in the [Tabix](https://github.com/tabixio/tabix) project.

Features:

- Works with ClickHouse directly from the browser, without the need to install additional software.
- Query editor with syntax highlighting.
- Auto-completion of commands.
- Tools for graphical analysis of query execution.
- Color scheme options.

[Tabix documentation](https://tabix.io/doc/).

### HouseOps

[HouseOps](https://github.com/HouseOps/HouseOps) is a UI/IDE for OSX, Linux and Windows.

Features:

- Query builder with syntax highlighting. View the response in a table or JSON view.
- Export query results as CSV or JSON.
- List of processes with descriptions. Write mode. Ability to stop (`KILL`) a process.
- Database graph. Shows all tables and their columns with additional information.
- Quick view of the column size.
- Server configuration.

The following features are planned for development:

- Database management.
- User management.
- Real-time data analysis.
- Cluster monitoring.
- Cluster management.
- Monitoring replicated and Kafka tables.

### LightHouse

[LightHouse](https://github.com/VKCOM/lighthouse) is a lightweight web interface for ClickHouse.

Features:

- Table list with filtering and metadata.
- Table preview with filtering and sorting.
- Read-only queries execution.

### DBeaver

[DBeaver](https://dbeaver.io/) - universal desktop database client with ClickHouse support.

Features:

- Query development with syntax highlight and autocompletion.
- Table list with filters and metadata search.
- Table data preview.
- Full text search.

### clickhouse-cli

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) is an alternative command line client for ClickHouse, written in Python 3.

Features:
- Autocompletion.
- Syntax highlighting for the queries and data output.
- Pager support for the data output.
- Custom PostgreSQL-like commands.

## Commercial

### DataGrip

[DataGrip](https://www.jetbrains.com/datagrip/) is a database IDE from JetBrains with dedicated support for ClickHouse. It is also embedded into other IntelliJ-based tools: PyCharm, IntelliJ IDEA, GoLand, PhpStorm and others.

Features:

- Very fast code completion.
- ClickHouse syntax highlighting.
- Support for features specific to ClickHouse, for example nested columns, table engines.
- Data Editor.
- Refactorings.
- Search and Navigation.

[Original article](https://clickhouse.yandex/docs/en/interfaces/third-party/gui/) <!--hide-->
