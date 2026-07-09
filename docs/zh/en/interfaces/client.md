---
description: 'ClickHouse 命令行客户端的文档'
sidebar_label: 'ClickHouse 命令行客户端'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse 命令行客户端'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse 提供原生命令行客户端，可直接向 ClickHouse 服务器执行 SQL 查询。
它同时支持交互模式 (用于实时执行查询) 和批次模式 (用于脚本编写和自动化) 。
查询结果可以显示在终端中，也可以导出到文件，并支持所有 ClickHouse 输出[格式](formats.md)，例如 Pretty、CSV、JSON 等。

该客户端可通过进度条、已读取的行数、已处理的字节数以及查询执行时间，实时反馈查询执行进度。
它同时支持[命令行选项](#command-line-options)和[配置文件](#configuration_files)。

<div id="install">
  ## 安装
</div>

要下载 ClickHouse，请执行：

```bash
curl https://clickhouse.com/ | sh
```

如需一并安装，请运行：

```bash
sudo ./clickhouse install
```

更多安装选项，请参见 [安装 ClickHouse](../getting-started/install/install.mdx)。

不同的客户端和服务端版本彼此兼容，但某些功能在较旧版本的客户端中可能不可用。我们建议客户端和服务端使用相同版本。

<div id="run">
  ## 运行
</div>

:::note
如果你只是下载了 ClickHouse，但尚未安装，请使用 `./clickhouse client`，而不要使用 `clickhouse-client`。
:::

要连接到 ClickHouse 服务器，请运行：

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

根据需要指定其他连接信息：

| Option                           | Description                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `--port <port>`                  | ClickHouse 服务器 接受连接的端口。默认端口为 9440 (TLS) 和 9000 (非 TLS) 。请注意，ClickHouse 命令行客户端 使用的是原生协议，而不是 HTTP(S)。 |
| `-s [ --secure ]`                | 是否使用 TLS (通常会自动检测) 。                                                                                   |
| `-u [ --user ] <username>`       | 以哪个数据库用户身份连接。默认以 `default` 用户连接。                                                                       |
| `--password <password>`          | 数据库用户的密码。你也可以在配置文件中为连接指定密码。如果未指定密码，客户端会提示你输入。                                                          |
| `-c [ --config ] <path-to-file>` | ClickHouse 命令行客户端 配置文件的位置；如果它不在默认位置之一，请使用此选项指定。参见[配置文件](#configuration_files)。                         |
| `--connection <name>`            | [配置文件](#connection-credentials)中预先配置的连接信息名称。                                                           |

有关完整的命令行选项列表，请参见[命令行选项](#command-line-options)。

<div id="connecting-cloud">
  ### 连接到 ClickHouse Cloud
</div>

您可以在 ClickHouse Cloud 控制台中查看 ClickHouse Cloud 服务的详细信息。选择要连接的服务，然后点击 **Connect**：

<Image img={cloud_connect_button} size="md" alt="ClickHouse Cloud 服务连接按钮" />

<br />

<br />

选择 **Native**，即可看到详细信息以及 `clickhouse-client` 命令示例：

<Image img={connection_details_native} size="md" alt="ClickHouse Cloud Native TCP 连接详细信息" />

<div id="connection-credentials">
  ### 在配置文件中存储连接信息
</div>

你可以将一个或多个 ClickHouse 服务器的连接信息存储在[配置文件](#configuration_files)中。

格式如下：

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

更多信息请参见[配置文件](#configuration_files)部分。

:::note
为便于聚焦查询语法，以下示例均省略了连接信息 (`--host`、`--port` 等) 。使用这些命令时，请记得补上。
:::

<div id="interactive-mode">
  ## 交互模式
</div>

<div id="using-interactive-mode">
  ### 使用交互模式
</div>

要以交互模式运行 ClickHouse，只需执行：

```bash
clickhouse-client
```

这会打开读取-求值-输出循环 (REPL) ，你就可以开始交互式输入 SQL 查询了。
连接后，你会看到一个提示符，可在其中输入查询：

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

在交互模式下，默认输出格式为 `PrettyCompact`。
你可以在查询的 `FORMAT` 子句中更改格式，或通过指定 `--format` 命令行选项来更改。
要使用 Vertical 格式，可以使用 `--vertical`，或在查询末尾指定 `\G`。
在这种格式下，每个值都会单独打印在一行上，这对于宽表很方便。

在交互模式下，默认情况下，按下 `Enter` 时会执行已输入的内容。
查询末尾不必加分号。

你可以使用 `-m, --multiline` 参数启动客户端。
要输入多行查询，请在换行符前输入反斜杠 `\`。
按下 `Enter` 后，系统会提示你输入查询的下一行。
要执行查询，请以分号结束并按下 `Enter`。

ClickHouse 命令行客户端 基于 `replxx` (类似于 `readline`) ，因此支持常见的键盘快捷键，并会保留历史记录。
默认情况下，历史记录会写入 `~/.clickhouse-client-history`。

要退出客户端，请按 `Ctrl+D`，或者输入以下任一内容来代替查询：

* `exit` 或 `exit;`
* `quit` 或 `quit;`
* `q`、`Q` 或 `:q`
* `logout` 或 `logout;`

<div id="getting-help">
  ### 获取帮助
</div>

你无需离开客户端，即可查阅系统中任意函数、表引擎、数据类型、格式、设置及其他组件的文档。输入 `help` 并跟上名称即可 (`/help`、`man` 和 `/man` 这几种等效形式也可用) ：

```text
help domainWithoutWWW
```

该查找不区分大小写，并会查询 [`system.documentation`](../operations/system-tables/documentation.md) 表。匹配到的文档会在终端中以 Markdown 渲染显示，包括粗体/斜体文本、表格以及带语法高亮的代码块。当多个组件共用同一个名称时 (例如 `file`，它既是函数也是一种表引擎) ，它们都会显示出来。

当没有任何内容能精确匹配时，客户端会列出相似的名称 (允许存在拼写错误) 以及其文档中提到该词的组件：

```text
help maxx_threads
```

单独输入 `help` 会显示简短的用法概述。

<div id="processing-info">
  ### 查询处理信息
</div>

处理查询时，客户端会显示：

1. Progress，默认每秒最多更新 10 次。
   对于执行很快的查询，进度可能还没来得及显示。
2. 解析后得到的格式化后的查询，用于调试。
3. 以指定格式显示的结果。
4. 结果的行数、耗时，以及查询处理的平均速度。
   所有数据量均指未压缩数据。

你可以按 `Ctrl+C` 取消长时间运行的查询。
不过，你仍需稍等片刻，让服务器中止该请求。
在某些阶段，查询无法取消。
如果你不等待而再次按下 `Ctrl+C`，客户端将退出。

ClickHouse 命令行客户端 支持在查询时传入外部数据 (外部临时表) 。
有关更多信息，请参见 [查询处理的外部数据](../engines/table-engines/special/external-data.md) 一节。

<div id="cli_aliases">
  ### 别名
</div>

你可以在 REPL 中使用以下别名：

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - 重复上一条查询语句

<div id="keyboard_shortcuts">
  ### 键盘快捷键
</div>

* `Alt (Option) + Shift + e` - 用当前查询打开编辑器。也可以通过环境变量 `EDITOR` 指定要使用的编辑器。默认使用 `vim`。
* `Alt (Option) + #` - 注释当前行。
* `Ctrl + r` - 模糊搜索历史记录。

包含所有可用键盘快捷键的完整列表请参见 [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262)。

:::tip
要在 MacOS 上正确配置 Meta 键 (Option) ：

iTerm2：依次前往 Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key，然后点击 Esc+
:::

<div id="batch-mode">
  ## 批次模式
</div>

<div id="using-batch-mode">
  ### 使用批次模式
</div>

你可以不以交互方式使用 ClickHouse 命令行客户端，而是以批次模式运行它。
在批次模式下，ClickHouse 执行一条查询后会立即退出——没有交互式提示符，也不会进入循环。

你可以像这样指定单条查询：

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

您还可以使用 `--query` 命令行选项：

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

您可以通过 `stdin` 传入查询：

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

假设存在一张表 `messages`，你也可以在命令行中插入数据：

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

当指定 `--query` 时，任何输入都会在一个换行符后追加到该请求中。

<div id="cloud-example">
  ### 将 CSV 文件插入远程 ClickHouse 服务
</div>

本示例将样本数据集 CSV 文件 `cell_towers.csv` 插入到 `default` 数据库中现有的 `cell_towers` 表中：

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### 从命令行插入数据的示例
</div>

可以通过多种方式从命令行插入数据。
下面的示例使用批次模式将两行 CSV 数据插入 ClickHouse 表：

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

在下面的示例中，`cat <<_EOF` 会启动一个 Heredoc，它会读取后续所有内容，直到再次遇到 `_EOF`，然后将其输出：

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

在下面的示例中，使用 `cat` 将 file.csv 的内容输出到 stdout，并通过管道传给 `clickhouse-client` 作为输入：

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

在批次模式下，默认的数据[格式](formats.md)为 `TabSeparated`。
如上例所示，您可以在查询的 `FORMAT` 子句中指定格式。

<div id="cli-queries-with-parameters">
  ## 带参数的查询
</div>

您可以在查询中指定参数，并通过命令行选项为其传递值。
这样可以避免在客户端用特定的动态值拼接查询语句。
例如：

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

也可在[交互式会话](#interactive-mode)中设置参数：

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### 查询语法
</div>

在查询中，将要通过命令行参数填入的值放在花括号中，格式如下：

```sql
{<name>:<data type>}
```

| 参数          | 描述                                                                                                                                                                                                                                                                                     |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | 占位符标识符。对应的命令行选项为 `--param_<name> = value`。                                                                                                                                                                                                                                             |
| `data type` | 参数的[数据类型](../sql-reference/data-types/index.md)。 <br /><br />例如，像 `(integer, ('string', integer))` 这样的数据结构，其数据类型可以是 `Tuple(UInt8, Tuple(String, UInt8))` (也可以使用其他 [integer](../sql-reference/data-types/int-uint.md) 类型) 。 <br /><br />表名、数据库名和列名也可以作为参数传递，此时需要使用 `Identifier` 作为数据类型。 |

<div id="cli-queries-with-parameters-examples">
  ### 示例
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## AI 驱动的 SQL 生成
</div>

ClickHouse 命令行客户端 内置了 AI 辅助功能，可根据自然语言描述生成 SQL 查询。此功能可帮助用户在无需深入掌握 SQL 的情况下编写复杂查询。

如果设置了 `OPENAI_API_KEY` 或 `ANTHROPIC_API_KEY` 环境变量，AI 辅助功能即可开箱即用。有关更高级的配置，请参阅[配置](#ai-sql-generation-configuration)部分。

<div id="ai-sql-generation-usage">
  ### 用法
</div>

如需使用 AI SQL 生成功能，请在自然语言查询前加上 `??`：

```bash
:) ?? show all users who made purchases in the last 30 days
```

AI 将会：

1. 自动探索你的数据库 schema
2. 根据发现的表和列生成合适的 SQL
3. 立即执行生成的查询

<div id="cli-queries-with-parameters-examples">
  ### 示例
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### 配置
</div>

AI SQL 生成功能需要在您的 ClickHouse 客户端 配置文件中设置 AI 提供商。您可以使用 OpenAI、Anthropic 或任何与 OpenAI 兼容的 API 服务。

<div id="ai-sql-generation-fallback">
  #### 基于环境变量的回退机制
</div>

如果在配置文件中未指定任何 AI 配置，ClickHouse 客户端 会自动尝试使用环境变量：

1. 首先检查 `OPENAI_API_KEY` 环境变量
2. 如果未找到，则检查 `ANTHROPIC_API_KEY` 环境变量
3. 如果两者都未找到，AI 功能将被禁用

这样无需配置文件也能快速完成设置：

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### 配置文件
</div>

如需更灵活地控制 AI 设置，请在以下位置的 ClickHouse 客户端 配置文件中进行配置：

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (如果未设置 `XDG_CONFIG_HOME`，则为 `~/.config/clickhouse/config.xml`)  (XML 格式)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (如果未设置 `XDG_CONFIG_HOME`，则为 `~/.config/clickhouse/config.yaml`)  (YAML 格式)
* `~/.clickhouse-client/config.xml` (XML 格式，旧版路径)
* `~/.clickhouse-client/config.yaml` (YAML 格式，旧版路径)
* 或使用 `--config-file` 指定自定义路径

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- 必填：你的 API 密钥（也可通过环境变量设置） -->
            <api_key>your-api-key-here</api_key>

            <!-- 必填：提供商类型（openai、anthropic） -->
            <provider>openai</provider>

            <!-- 要使用的模型（默认值因提供商而异） -->
            <model>gpt-4o</model>

            <!-- 可选：用于 OpenAI 兼容服务的自定义 API 端点 -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- schema 探索设置 -->
            <enable_schema_access>true</enable_schema_access>

            <!-- 生成参数 -->
            <!-- 可选：仅当在此处设置时，temperature 才会发送给模型。
                 默认会省略，因为某些模型不接受此参数。 -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- 可选：自定义系统提示 -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # 必填：你的 API 密钥（也可通过环境变量设置）
      api_key: your-api-key-here

      # 必填：提供商类型（openai、anthropic）
      provider: openai

      # 要使用的模型
      model: gpt-4o

      # 可选：用于 OpenAI 兼容服务的自定义 API 端点
      # base_url: https://openrouter.ai/api

      # 启用 schema 访问 - 允许 AI 查询数据库/表信息
      enable_schema_access: true

      # 生成参数
      # 仅当在此处设置时，temperature 才会发送给模型；默认会省略
      # 因为某些模型不接受此参数。
      # temperature: 0.0    # 控制随机性（0.0 = 确定性）
      max_tokens: 1000      # 最大响应长度
      timeout_seconds: 30   # 请求超时
      max_steps: 10         # schema 探索的最大步数

      # 可选：自定义系统提示
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**使用 OpenAI 兼容 API (例如 OpenRouter) ：**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**最简配置示例：**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### 参数
</div>

<details>
  <summary>必需参数</summary>

  * `api_key` - AI 服务的 API 密钥。如果已通过环境变量设置，则可省略：
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * 注意：配置文件中的 API 密钥优先于环境变量
  * `provider` - AI 提供商：`openai` 或 `anthropic`
    * 如果省略，会根据可用的环境变量自动回退
</details>

<details>
  <summary>模型配置</summary>

  * `model` - 要使用的模型 (默认：提供商特定的默认模型)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo` 等
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229` 等
    * OpenRouter: 使用其模型命名方式，例如 `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>连接设置</summary>

  * `base_url` - 用于与 OpenAI 兼容服务的自定义 API 端点 (可选)
  * `timeout_seconds` - 请求超时时间 (秒)  (默认：`30`)
</details>

<details>
  <summary>schema 探索</summary>

  * `enable_schema_access` - 允许 AI 探索数据库 schema (默认：`true`)
  * `max_steps` - schema 探索期间工具调用的最大步数 (默认：`10`)
</details>

<details>
  <summary>生成参数</summary>

  * `temperature` - 控制随机性，0.0 = 确定性，1.0 = 更具创造性。默认会省略该参数，只有在显式设置时才会发送给模型，因为某些模型会拒绝此参数。
  * `max_tokens` - 响应的最大长度 (以标记为单位)  (默认：`1000`)
  * `system_prompt` - AI 的自定义指令 (可选)
</details>

<div id="ai-sql-generation-how-it-works">
  ### 工作原理
</div>

AI SQL 生成器采用多步流程：

<VerticalStepper headerLevel="list">
  1. **Schema 发现**

  AI 使用内置工具探索你的数据库：

  * 列出可用的数据库
  * 发现相关数据库中的表
  * 通过 `CREATE TABLE` 语句检查表结构

  2. **查询生成**

  基于发现的 schema，AI 会生成满足以下条件的 SQL：

  * 符合你的自然语言意图
  * 使用正确的表名和列名
  * 应用适当的 JOIN 和聚合

  3. **执行**

  生成的 SQL 会自动执行，并显示结果
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### 限制
</div>

* 需要有效的互联网连接
* API 的使用受 AI 提供商的速率限制和费用约束
* 复杂查询可能需要多次调整
* AI 对 schema 信息仅有只读访问权限，无法访问实际数据

<div id="ai-sql-generation-security">
  ### 安全
</div>

* API 密钥绝不会发送到 ClickHouse 服务器
* AI 只能看到 schema 信息 (表名/列名和类型) ，看不到实际数据
* 所有生成的查询都会遵循您当前的数据库权限

<div id="connection_string">
  ## 连接字符串
</div>

<div id="ai-sql-generation-usage">
  ### 用法
</div>

ClickHouse 客户端 还支持使用连接字符串连接到 ClickHouse 服务器，其格式与 [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/)、[PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) 和 [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri) 类似。语法如下：

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| 组件 (均为可选)          | 描述                                                                    | 默认值              |
| ------------------ | --------------------------------------------------------------------- | ---------------- |
| `user`             | 数据库用户名。                                                               | `default`        |
| `password`         | 数据库用户的密码。如果指定了 `:` 且密码为空，客户端将提示输入用户密码。                                | -                |
| `hosts_and_ports`  | 主机及可选端口列表 `host[:port] [, host:[port]], ...`。                         | `localhost:9000` |
| `database`         | 数据库名称。                                                                | `default`        |
| `query_parameters` | 键值对列表 `param1=value1[,&param2=value2], ...`。对于某些参数，无需提供值。参数名称和值区分大小写。 | -                |

<div id="connection-string-notes">
  ### 注意事项
</div>

如果已在连接字符串中指定用户名、密码或数据库，就不能再通过 `--user`、`--password` 或 `--database` 指定 (反之亦然) 。

host 部分既可以是主机名，也可以是 IPv4 或 IPv6 地址。
IPv6 地址应置于 `[]` 中：

```text
clickhouse://[2001:db8::1234]
```

连接字符串可以包含多个主机。
ClickHouse 客户端会按顺序 (从左到右) 尝试连接这些主机。
一旦连接建立，就不会再尝试连接其余主机。

连接字符串必须作为 `clickHouse-client` 的第一个参数指定。
除 `--host` 和 `--port` 外，连接字符串还可以与任意数量的其他[命令行选项](#command-line-options)结合使用。

`query_parameters` 允许使用以下键：

| 键                 | 说明                                                                           |
| ----------------- | ---------------------------------------------------------------------------- |
| `secure` (或 `s`)  | 如果指定，客户端将通过安全连接 (TLS) 连接到服务器。请参见[命令行选项](#command-line-options)中的 `--secure`。 |

**百分比编码**

以下参数中的非美式 ASCII 字符、空格和特殊字符必须进行[百分比编码](https://en.wikipedia.org/wiki/URL_encoding)：

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### 示例
</div>

连接到 `localhost` 的 9000 端口，并执行查询 `SELECT 1`。

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

使用用户 `john` 和密码 `secret` 连接到 `localhost`，主机为 `127.0.0.1`，端口为 `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

以 `default` 用户连接到 `localhost` 主机 (IPv6 地址为 `[::1]`) ，端口为 `9000`。

```bash
clickhouse-client clickhouse://[::1]:9000
```

以多行模式连接到 `localhost` 上的 9000 端口。

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

使用 `default` 用户通过 9000 端口连接到 `localhost`。

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

连接到 `localhost` 的 9000 端口，并默认使用 `my_database` 数据库。

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

连接到端口 9000 上的 `localhost`，默认使用连接字符串中指定的 `my_database` database，并通过简写 `s` parameter 启用 secure connection。

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

使用默认主机、默认端口、`default` 用户和 `default` 数据库进行连接。

```bash
clickhouse-client clickhouse:
```

使用默认主机和默认端口，以用户 `my_user` 身份连接，无需密码。

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

使用电子邮件作为用户名连接到 `localhost`。`@` 符号会进行百分号编码，表示为 `%40`。

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

连接到两个主机中的一个：`192.168.1.15`、`192.168.1.25`。

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## 查询 ID 格式
</div>

在交互模式下，ClickHouse 客户端 会为每条查询显示查询 ID。默认情况下，ID 的格式如下：

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

可以在配置文件的 `query_id_formats` 标签中指定自定义格式。格式字符串中的 `{query_id}` 占位符会替换为查询 ID。该标签中可包含多个格式字符串。
此功能可用于生成 URL，以便对查询进行性能分析。

**示例**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

使用上述配置后，查询的 ID 将以以下格式显示：

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## 配置文件
</div>

ClickHouse 客户端会使用以下列表中第一个存在的文件：

* 通过 `-c [ -C, --config, --config-file ]` 参数指定的文件。
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (如果未设置 `XDG_CONFIG_HOME`，则使用 `~/.config/clickhouse/config.[xml|yaml|yml]`)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

可参考 ClickHouse 仓库中的示例配置文件：[`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## 环境变量选项
</div>

用户名、密码和主机可通过环境变量 `CLICKHOUSE_USER`、`CLICKHOUSE_PASSWORD` 和 `CLICKHOUSE_HOST` 设置。
如果指定了命令行参数 `--user`、`--password` 或 `--host`，或者指定了[连接字符串](#connection_string)，则其优先级高于环境变量。

<div id="command-line-options">
  ## 命令行选项
</div>

所有命令行选项都可以直接在命令行中指定，也可以在[配置文件](#configuration_files)中设置默认值。

<div id="command-line-options-general">
  ### 常规选项
</div>

| Option                                              | Description                                               | Default                 |
| --------------------------------------------------- | --------------------------------------------------------- | ----------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | 如果客户端的配置文件不在默认位置之一，请指定其路径。参见[配置文件](#configuration_files)。 | -                       |
| `--help`                                            | 打印用法摘要并退出。与 `--verbose` 一起使用时，会显示所有可用选项，包括查询设置。           | -                       |
| `--history_file <path-to-file>`                     | 包含命令历史记录的文件路径。                                            | -                       |
| `--history_max_entries`                             | 历史记录文件中的最大条目数。                                            | `1000000` (100 万)       |
| `--prompt <prompt>`                                 | 指定自定义提示符。                                                 | server 的 `display_name` |
| `--verbose`                                         | 提高输出的详细程度。                                                | -                       |
| `-V [ --version ]`                                  | 打印版本信息并退出。                                                | -                       |

<div id="command-line-options-connection">
  ### 连接选项
</div>

| Option                               | Description                                                                                                                                                                                                            | Default                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `--connection <name>`                | 配置文件中预配置的连接详细信息名称。请参见[连接凭据](#connection-credentials)。                                                                                                                                                                  | -                                                                                  |
| `-d [ --database ] <database>`       | 选择此连接默认使用的数据库。                                                                                                                                                                                                         | 服务器设置中的当前数据库 (默认为 `default`)                                                       |
| `-h [ --host ] <host>`               | 要连接的 ClickHouse 服务器的主机名。可以是主机名，也可以是 IPv4 或 IPv6 地址。可通过多个参数传递多个主机。                                                                                                                                                      | `localhost`                                                                        |
| `--jwt <value>`                      | 使用 JSON Web Token (JWT) 进行身份验证。<br /><br />服务端 JWT 授权仅在 ClickHouse Cloud 中可用。                                                                                                                                          | -                                                                                  |
| `login`                              | 调用设备授权 OAuth 流程，以通过 IdP 进行身份验证。<br /><br />对于 ClickHouse Cloud 主机，OAuth 变量会自动推断；否则必须通过 `--oauth-url`、`--oauth-client-id` 和 `--oauth-audience` 提供。                                                                      | -                                                                                  |
| `--no-warnings`                      | 禁止客户端连接到服务器时显示来自 `system.warnings` 的警告。                                                                                                                                                                                | -                                                                                  |
| `--no-server-client-version-message` | 在客户端连接到服务器时，禁止显示服务端与客户端版本不匹配的消息。                                                                                                                                                                                       | -                                                                                  |
| `--password <password>`              | 数据库用户的密码。你也可以在配置文件中为连接指定密码。如果未指定密码，客户端会提示你输入。                                                                                                                                                                          | -                                                                                  |
| `--port <port>`                      | 服务器接受连接的端口。默认端口为 9440 (TLS) 和 9000 (无 TLS) 。<br /><br />注意：客户端使用的是原生协议，而不是 HTTP(S)。                                                                                                                                    | 如果指定了 `--secure`，则为 `9440`；否则为 `9000`。如果主机名以 `.clickhouse.cloud` 结尾，则始终默认为 `9440`。 |
| `-s [ --secure ]`                    | 是否使用 TLS。<br /><br />连接到 9440 端口 (默认安全端口) 或 ClickHouse Cloud 时会自动启用。<br /><br />你可能需要在[配置文件](#configuration_files)中配置 CA 证书。可用的配置设置与[服务端 TLS 配置](../operations/server-configuration-parameters/settings.md#openssl)相同。 | 连接到 9440 端口或 ClickHouse Cloud 时自动启用                                                |
| `--ssh-key-file <path-to-file>`      | 包含用于与服务器进行身份验证的 SSH 私钥的文件。                                                                                                                                                                                             | -                                                                                  |
| `--ssh-key-passphrase <value>`       | `--ssh-key-file` 中指定的 SSH 私钥的口令短语。                                                                                                                                                                                     | -                                                                                  |
| `--tls-sni-override <server name>`   | 如果使用 TLS，则在握手中传递的服务器名称 (SNI) 。                                                                                                                                                                                         | 通过 `-h` 或 `--host` 提供的主机。                                                          |
| `-u [ --user ] <username>`           | 用于连接的数据库用户。                                                                                                                                                                                                            | `default`                                                                          |

:::note
除了 `--host`、`--port`、`--user` 和 `--password` 选项外，客户端还支持[连接字符串](#connection_string)。
:::

<div id="command-line-options-query">
  ### 查询选项
</div>

| 选项                              | 说明                                                                                                                                                                                                                                                                                                                           |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | [带参数的查询](#cli-queries-with-parameters)中参数的替换值。                                                                                                                                                                                                                                                                               |
| `-q [ --query ] <query>`        | 要在批次模式下运行的查询。可多次指定 (`--query "SELECT 1" --query "SELECT 2"`) ，也可一次指定多个以分号分隔的查询 (`--query "SELECT 1; SELECT 2;"`) 。在后一种情况下，使用非 `VALUES` 格式的 `INSERT` 查询必须以空行分隔。 <br /><br />也可以不带参数直接指定单个查询：`clickhouse-client "SELECT 1"` <br /><br />不能与 `--queries-file` 一起使用。                                                             |
| `--queries-file <path-to-file>` | 包含查询的文件路径。`--queries-file` 可指定多次，例如 `--queries-file queries1.sql --queries-file queries2.sql`。 <br /><br />不能与 `--query` 一起使用。                                                                                                                                                                                               |
| `-m [ --multiline ]`            | 如果指定，则允许输入多行查询 (按 Enter 时不会发送查询) 。只有以分号结束时，查询才会发送。                                                                                                                                                                                                                                                                           |
| `--inline-insert-data`          | 将 `INSERT ... VALUES` (以及其他内联格式) 的数据按原样放在查询文本中发送，而不是先将数据转换为原生格式的块。服务器会自行解析内联数据，从而避免把表结构和列默认值回传给客户端的往返开销。对于通过原生协议进行的大量小型插入操作，这可以提升性能。会自动将 [`send_table_structure_on_insert_with_inline_data`](/zh/operations/settings/settings#send_table_structure_on_insert_with_inline_data) 设置为 `0`。不能与内联数据和外部数据 (来自 stdin 或 `INFILE`) 同时使用。 |

<div id="command-line-options-query-settings">
  ### 查询设置
</div>

查询设置可以通过客户端的命令行选项指定，例如：

```bash
$ clickhouse-client --max_threads 1
```

有关设置的列表，请参见[设置](../operations/settings/settings.md)。

<div id="command-line-options-formatting">
  ### 格式化选项
</div>

| 选项                                | 说明                                                                                                                                                                                                                                   | 默认值                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------ |
| `-f [ --format ] <format>`        | 使用指定格式输出结果。<br /><br />支持的格式列表请参见[输入和输出数据格式](formats.md)。                                                                                                                                                                            | `TabSeparated`                       |
| `--pager <command>`               | 将所有输出通过管道传递给此命令。通常为 `less` (例如，使用 `less -S` 显示宽结果集) 或类似命令。                                                                                                                                                                           | -                                    |
| `-E [ --vertical ]`               | 使用 [Vertical 格式](/zh/interfaces/formats/Vertical) 输出结果。这与 `–-format Vertical` 相同。在这种格式下，每个值都会单独打印成一行，因此在显示宽表时很有帮助。                                                                                                                      | -                                    |
| `--echo [ <bool> ]`               | 在执行前打印每个查询。接受一个可选的布尔值。                                                                                                                                                                                                               | 交互模式下为 `true`，非交互式 (批次) 模式下为 `false` |
| `--echo-formatted [ <bool> ]`     | 对回显的查询进行格式化。接受一个可选的布尔值。                                                                                                                                                                                                              | 交互模式下为 `true`，非交互式 (批次) 模式下为 `false` |
| `--echo-query-id [ <bool> ]`      | 在执行前打印查询 ID。接受一个可选的布尔值。                                                                                                                                                                                                              | 交互模式下为 `true`，非交互式 (批次) 模式下为 `false` |
| `--echo-query-separator <string>` | 在格式化后的回显查询前打印此分隔符 (需要 `--echo-formatted`) ，以便更容易区分输入的查询和重新格式化后的回显。                                                                                                                                                                   | 空 (禁用)                               |
| `--highlight [ --hilite ] <bool>` | 切换命令提示符和回显查询的语法高亮显示。                                                                                                                                                                                                                 | `true`                               |
| `--hints <bool>`                  | 当光标位于输入末尾时，显示输入时自动补全提示 (内联“幽灵”文本) ，用于展示最佳匹配建议。使用向上/向下 (或 Ctrl-Up/Ctrl-Down) 在提示间切换；使用 Tab 或 Right 接受内联提示；`Enter` 只有在已显式选择某个提示后才会接受该提示，否则会执行查询；`Tab` 还会打开经典补全列表。需要 `--highlight` (提示需要颜色) 以及建议机制 (因此 `--disable_suggestion` 也会将其关闭) 。 | `true`                               |

<div id="command-line-options-execution-details">
  ### 执行详情
</div>

| 选项                               | 说明                                                                                                                                                                                                           | 默认值                               |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
| `--chime [N]`                    | 当查询运行至少 `N` 秒后结束时 (无论成功还是出错) ，向 `stderr` 写入 `BEL` 控制字符。仅当 `stderr` 连接到终端 (TTY) 时才会输出；重定向 `stderr` (例如 `2>err.log`) 会抑制该输出，而重定向 `stdout` (例如 `> result.tsv`) 则不会。传入不带值的 `--chime` 会使用默认阈值。设置 `--chime 0` 可禁用。 | `5` 秒                             |
| `--enable-progress-table-toggle` | 启用按控制键 (空格键) 切换进度表。仅适用于已启用进度表打印的交互模式。                                                                                                                                                                        | `enabled`                         |
| `--hardware-utilization`         | 在进度条中打印硬件利用率信息。                                                                                                                                                                                              | -                                 |
| `--memory-usage`                 | 如果指定，则在非交互模式下将内存使用情况打印到 `stderr`。 <br /><br />可能的值： <br />• `none` - 不打印内存使用情况 <br />• `default` - 打印字节数 <br />• `readable` - 以易读格式打印内存使用情况                                                                  | -                                 |
| `--print-profile-events`         | 打印 `ProfileEvents` 数据包。                                                                                                                                                                                      | -                                 |
| `--progress`                     | 打印查询执行进度。 <br /><br />可能的值： <br />• `tty\|on\|1\|true\|yes` - 在交互模式下输出到终端 <br />• `err` - 在非交互模式下输出到 `stderr` <br />• `off\|0\|false\|no` - 禁用进度输出                                                           | 交互模式下为 `tty`，非交互 (批处理) 模式下为 `off` |
| `--progress-table`               | 在查询执行期间打印显示动态变化指标的进度表。 <br /><br />可能的值： <br />• `tty\|on\|1\|true\|yes` - 在交互模式下输出到终端 <br />• `err` - 在非交互模式下输出到 `stderr` <br />• `off\|0\|false\|no` - 禁用进度表                                               | 交互模式下为 `tty`，非交互 (批处理) 模式下为 `off` |
| `--stacktrace`                   | 打印异常的堆栈跟踪。                                                                                                                                                                                                   | -                                 |
| `-t [ --time ]`                  | 在非交互模式下将查询执行时间打印到 `stderr` (用于性能测试) 。                                                                                                                                                                        | -                                 |