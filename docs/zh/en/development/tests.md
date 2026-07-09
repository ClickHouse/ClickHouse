---
description: '测试 ClickHouse 及运行测试套件指南'
sidebar_label: '测试'
sidebar_position: 40
slug: /development/tests
title: '测试 ClickHouse'
doc_type: 'guide'
---

<div id="test-types">
  ## 测试类型
</div>

ClickHouse 中有以下几类测试：

* [功能测试](#functional-tests)——一组查询和脚本，包含以下几个彼此重叠的子集
  * [快速测试](#running-fast-tests)——最小子集
  * [无状态测试](#running-stateless-tests)，无需向数据库填充数据
  * 无法并行运行的顺序测试
* [集成测试](#integration-tests)，由 `pytest` 在集群中运行
* [单元测试](#unit-tests)
* [性能测试](#performance-tests)
* [构建测试](#build-tests)
* [Sanitizers](#sanitizers)
* [Fuzzers](#fuzzing)
  以及其他一些测试，详见下文各节。

<div id="functional-tests">
  ## 功能测试
</div>

功能测试是最简单、最方便使用的一类测试。
ClickHouse 的大多数功能都可以通过功能测试进行验证，因此，对于 ClickHouse 代码中所有适合用这种方式测试的改动，都必须编写功能测试。

每个功能测试都会向正在运行的 ClickHouse 服务器发送一个或多个查询，并将结果与参考结果进行比较。

测试位于 `./tests/queries` 目录中。

每个测试都属于以下两种类型之一：`.sql` 和 `.sh`。

* `.sql` 测试是通过管道传给 `clickhouse-client` 的简单 SQL 脚本。
* `.sh` 测试是自行运行的脚本。

通常应优先使用 SQL 测试，而不是 `.sh` 测试。
只有在必须测试某些无法仅通过纯 SQL 覆盖的功能时，才应使用 `.sh` 测试，例如将某些输入数据通过管道传给 `clickhouse-client`，或测试 `clickhouse-local`。

:::note
测试 `DateTime` 和 `DateTime64` 数据类型时，一个常见错误是误以为服务器会使用特定时区 (例如 &quot;UTC&quot;) 。实际并非如此，在 CI 测试运行中，时区
会被刻意随机化。最简单的解决方法是为测试值显式指定时区，例如 `toDateTime64(val, 3, 'Europe/Amsterdam')`。
:::

<div id="running-a-test-locally">
  ### 在本地运行测试
</div>

在本地启动 ClickHouse 服务器，并监听默认端口 (9000) 。
例如，要运行测试 `01428_hash_set_nan_key`，请切换到仓库目录并运行以下命令：

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

测试结果 (`stderr` 和 `stdout`) 会写入 `01428_hash_set_nan_key.[stderr|stdout]` 文件，这些文件与测试文件位于同一目录 (例如，对于 `queries/0_stateless/foo.sql`，输出文件将位于 `queries/0_stateless/foo.stdout`) 。

有关 `clickhouse-test` 的所有选项，请参见 `tests/clickhouse-test --help`。
你可以运行所有测试，也可以通过为测试名称指定过滤器来运行部分测试：`./clickhouse-test substring`。
此外，还可以选择并行运行测试，或按随机顺序运行测试。

<div id="running-tests-on-macos">
  #### 在 macOS (Darwin) 上运行测试
</div>

许多功能测试都会调用 GNU 命令行工具 (`timeout`、`head`、`sed`、`grep`、`date` 等) 。macOS 自带的是这些工具的 BSD 版本，而它们的行为和选项有所不同 (例如，BSD `head` 不支持 `head -c 1G`，BSD `ps` 没有 `--` 长选项，甚至完全没有 `timeout`) 。如果使用 BSD 工具来运行这些测试，就会出现一些无关的失败。

macOS CI 运行器会通过 Homebrew 安装 GNU 工具，并让它们在 `PATH` 中排在 BSD 工具之前。在本地也请按同样方式配置：

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils` 提供 GNU `timeout`、`head`、`date` 等工具；`gnu-sed` 和 `grep` 提供 GNU `sed` 和 `grep`。完成后，`which timeout head sed grep` 应该会指向 `gnubin` 路径。

<div id="running-fast-tests">
  ### 运行快速测试
</div>

你可能需要一台配置较强的机器来运行一部分测试 (称为 &quot;快速测试&quot;) 。以下步骤已在配备 100 GB 存储的 `t3.2xlarge` AWS amd64 Ubuntu 实例上验证可行。

1. 安装前置条件，然后重新登录。

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. 获取源代码。

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. 编译代码并运行 &quot;快速测试&quot;。

```sh
python -m ci.praktika run fast
```

你应该会看到

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

如果你需要在无人值守的情况下继续运行，可以使用 `nohup` 或 `disown`，以便在 `ssh` 连接断开后仍继续运行。

<div id="running-stateless-tests">
  ### 运行无状态测试
</div>

运行无状态测试可能需要一台性能较强的机器。以下步骤已在配备 200 GB 存储的 `m7i.8xlarge` AWS amd64 Ubuntu 实例上验证可用。

1. 安装前置条件并重新登录。

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

2. 获取源代码。

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. 构建代码。

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. 运行可并行运行的无状态测试。

```sh
python -m ci.praktika run functional
```

你应该会看到

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

注意：`python -m ci.praktika run` 命令会运行一个特定的持续集成任务，更多关于 ClickHouse CI 的信息可参见[这里](continuous-integration.md#running-stateless-tests)。

<div id="adding-a-new-test">
  ### 添加新测试
</div>

要添加新测试，首先在 `queries/0_stateless` 目录中创建一个 `.sql` 或 `.sh` 文件。
然后使用 `clickhouse-client < 12345_test.sql > 12345_test.reference` 或 `./12345_test.sh > ./12345_test.reference` 生成对应的 `.reference` 文件。

测试只能在预先自动创建的 `test` 数据库中的表上执行 create、drop、select 等操作。
也可以使用临时表。

如需在本地搭建与 CI 相同的环境，请安装测试配置 (它们会使用 ZooKeeper mock 实现，并调整某些设置) 

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
测试应当：

* 尽可能精简：只创建最低限度所需的表、列以及复杂性，
* 尽量快速：不要超过几秒钟 (最好不到一秒) ，
* 保证正确且具备确定性：当且仅当被测功能未正常工作时才失败，
* 保持隔离/无状态：不要依赖环境和时序，
* 覆盖全面：涵盖零值、null 值、空集、异常等边界情况 (负向测试请使用语法 `-- { serverError xyz }` 和 `-- { clientError xyz }`) ，
* 在测试结束时清理表 (以防有残留) ，
* 确保其他测试不会测试相同内容 (即先 `grep` 一下) 。
  :::

<div id="templated-tests-with-jinja">
  ### 使用 Jinja 的模板化测试
</div>

只需在文件名后添加 `.j2` 后缀，就可以将 `.sql` 测试写成 [Jinja2](https://jinja.palletsprojects.com/) 模板，因此 `foo.sql` 会变成 `foo.sql.j2`。在运行测试之前，`clickhouse-test` 会先将模板渲染为普通的 `.sql` 脚本，然后执行生成的结果。

当一个测试需要对同一查询仅做少量变化并重复执行时，这种方式就很有用：可以通过循环从一个简洁的模板生成这些查询，而不必手动逐个编写。最常用的结构包括：

* `{% for ... %} ... {% endfor %}`：用于重复一个块，
* `{{ expression }}`：用于将某个值插入输出中，
* `-%}` 和 `{%-`：用于去除相邻空白，使生成的脚本保持整洁。

例如，下面这个模板：

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

渲染结果为：

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

预期输出既可以提供为普通的 `<name>.reference` 文件，其中包含完全展开的结果；也可以提供为 `<name>.reference.j2` 模板，`clickhouse-test` 会先以相同方式将其渲染后再进行比较。当预期输出也呈现重复模式时，请使用模板形式。更多示例请参见 `tests/queries/0_stateless/` 中现有的 `*.sql.j2` 文件。

<div id="restricting-test-runs">
  ### 限制测试运行
</div>

一个测试可以带有零个或多个*标签*，用来规定该测试在 CI 中可在哪些上下文中运行。

对于 `.sql` 测试，标签放在第一行，以 SQL 注释的形式写出：

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

对于 `.sh` 测试，标签写在第二行的注释中：

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

可用标签列表：

| 标签名                            | 作用                                                | 使用示例                                                          |
| ------------------------------ | ------------------------------------------------- | ------------------------------------------------------------- |
| `disabled`                     | 不运行该测试                                            |                                                               |
| `long`                         | 测试执行时间从 1 分钟延长到 10 分钟                             |                                                               |
| `deadlock`                     | 测试会在循环中长时间运行                                      |                                                               |
| `race`                         | 与 `deadlock` 相同。优先使用 `deadlock`                   |                                                               |
| `shard`                        | 要求 server 监听 `127.0.0.*`                          |                                                               |
| `distributed`                  | 与 `shard` 相同。优先使用 `shard`                         |                                                               |
| `global`                       | 与 `shard` 相同。优先使用 `shard`                         |                                                               |
| `zookeeper`                    | 测试运行需要依赖 Zookeeper 或 ClickHouse Keeper            | 测试使用 `ReplicatedMergeTree`                                    |
| `replica`                      | 与 `zookeeper` 相同。优先使用 `zookeeper`                 |                                                               |
| `no-fasttest`                  | 该测试不会在 [快速测试](#test-types) 中运行                    | 测试使用 `MySQL` 表引擎，而该引擎在快速测试中被禁用                                |
| `fasttest-only`                | 该测试仅在 [快速测试](#test-types) 中运行                     |                                                               |
| `no-[asan, tsan, msan, ubsan]` | 在启用了 [sanitizers](#sanitizers) 的构建中禁用该测试          | 测试在 QEMU 下运行，而 QEMU 与 sanitizers 不兼容                          |
| `no-replicated-database`       | 当默认 database 使用 `ReplicatedDatabaseEngine` 时禁用该测试 |                                                               |
| `no-ordinary-database`         | 当默认 database engine 为 `Ordinary` 时禁用该测试           |                                                               |
| `no-parallel`                  | 禁止其他测试与该测试并行运行                                    | 测试会读取 `system` 表，不变量可能被破坏                                     |
| `no-parallel-replicas`         | 启用并行副本时禁用该测试                                      |                                                               |
| `no-debug`                     | 在 Debug 构建中禁用该测试                                  |                                                               |
| `no-release`                   | 在 Release 构建中禁用该测试                                |                                                               |
| `no-darwin`                    | 在 macOS (Darwin) 上禁用该测试                           | 测试依赖 Linux 特有功能，例如 distributed queries、`procfs` 或 HTTP server |

还支持以下选项：`no-polymorphic-parts`、`no-random-settings`、`no-random-merge-tree-settings`、`no-backward-compatibility-check`、`no-cpu-x86_64`、`no-cpu-aarch64`、`no-cpu-ppc64le`、`no-s3-storage`。

除上述设置外，你还可以使用 `system.build_options` 中的 `USE_*` flag 来定义特定 ClickHouse feature 的使用情况。
例如，如果你的测试使用 MySQL 表，则应添加标签 `use-mysql`。

<div id="specifying-limits-for-random-settings">
  ### 为随机设置指定取值限制
</div>

测试可以为测试运行期间可随机化的设置指定允许的最小值和最大值。

对于 `.sh` 测试，如果指定了标签，限制会以注释形式写在标签所在行的旁边；如果未指定标签，则写在第二行：

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

对于 `.sql` 测试，标签以 SQL 注释的形式放在标签所在行的下一行，或放在第一行：

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

如果你只需要指定一个限制，另一个可以使用 `None`。

<div id="choosing-the-test-name">
  ### 选择测试名称
</div>

测试名称以五位数字前缀开头，后跟一个描述性名称，例如 `00422_hash_function_constexpr.sql`。
选择此前缀时，请在目录中找到当前已存在的最大前缀，并将其加一。

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

与此同时，可能还会添加一些带有相同数字前缀的测试，但这完全没问题，也不会引发任何问题，之后你也不需要再修改它。

<div id="checking-for-an-error-that-must-occur">
  ### 检查必须出现的错误
</div>

有时，你可能想测试错误的查询是否会触发服务器错误。为此，我们在 SQL 测试中支持使用特殊注释，形式如下：

```sql
SELECT x; -- { serverError 49 }
```

此测试用于确保 server 返回一个关于未知列 `x`、代码为 49 的 error。
如果没有报错，或者返回的是其他 error，测试就会失败。
如果你想确保错误发生在 client 端，请改用 `clientError` annotation。

不要检查 error message 的具体措辞，因为它今后可能会发生变化，导致测试无谓地失效。
只检查 error code。
如果现有 error code 不够精确，无法满足你的需求，可以考虑新增一个。

<div id="testing-a-distributed-query">
  ### 测试分布式查询
</div>

如果你想在 功能测试 中使用分布式查询，可以利用带有 `127.0.0.{1..2}` 地址的 `remote` 表函数，让 server 查询自身；或者也可以使用 server configuration file 中预定义的测试集群，例如 `test_shard_localhost`。
请记得在测试名称中加入 `shard` 或 `distributed` 字样，这样它才会在支持分布式查询的 server 配置下，于 CI 中以正确的配置运行。

<div id="working-with-temporary-files">
  ### 使用临时 File
</div>

有时在 shell 测试中，你可能需要临时创建一个文件来配合测试。
请记住，某些 CI 检查会并行运行测试，因此如果你在脚本中创建或删除临时 File 时没有使用唯一名称，可能会导致某些 CI 检查 (例如 Flaky) 失败。
为避免这种情况，你应该使用环境变量 `$CLICKHOUSE_TEST_UNIQUE_NAME`，为临时 File 指定一个当前运行测试专用的唯一名称。
这样你就可以确保，无论是在准备阶段创建的文件，还是在清理阶段删除的文件，都只会被该测试使用，而不会与其他并行运行的测试互相干扰。

<div id="known-bugs">
  ## 已知缺陷
</div>

如果已知某些缺陷能够通过功能测试轻松复现，我们会将预先编写好的功能测试放在 `tests/queries/bugs` 目录下。
这些缺陷修复后，相应的测试会移到 `tests/queries/0_stateless`。

<div id="integration-tests">
  ## 集成测试
</div>

集成测试可用于测试集群配置下的 ClickHouse，以及 ClickHouse 与 MySQL、Postgres、MongoDB 等其他服务器之间的交互。
它们可用于模拟网络分区、数据包丢失等情况。
这些测试在 Docker 中运行，并会创建多个装有不同软件的容器。

有关如何运行这些测试，请参阅 `tests/integration/README.md`。

请注意，ClickHouse 与第三方驱动程序的集成不在测试范围内。
此外，我们目前也没有针对自有 JDBC 和 ODBC 驱动程序的集成测试。

<div id="unit-tests">
  ## 单元测试
</div>

当你要测试的不是整个 ClickHouse，而是某个独立的库或类时，单元测试就很有用。
你可以通过 `ENABLE_TESTS` 这个 CMake 选项启用或禁用测试构建。
单元测试 (以及其他测试程序) 分布在代码各处的 `tests` 子目录中。
要运行单元测试，请输入 `ninja test`。
有些测试使用 `gtest`，但也有一些只是普通程序，在测试失败时会返回非零退出码。

如果代码已经由功能测试覆盖，就不一定还需要单元测试 (而且功能测试通常更简单，也更容易使用) 。

你可以直接调用可执行文件来运行单个 gtest 检查项，例如：

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## 性能测试
</div>

性能测试可用于测量并比较 ClickHouse 某些独立模块在合成查询上的性能表现。
性能测试位于 `tests/performance/`。
每个测试都由一个 `.xml` 文件表示，其中包含测试用例的描述。
测试通过 `docker/test/performance-comparison` 工具运行。调用方式请参阅 readme 文件。

每个测试都会在循环中运行一个或多个查询 (也可能包含多种参数组合) 。

如果你想在某种场景下提升 ClickHouse 的性能，并且这些改进可以通过简单查询观察到，强烈建议编写性能测试。
此外，当你新增或修改相对独立且不太冷门的 SQL 函数时，也建议编写性能测试。
在测试过程中使用 `perf top` 或其他 `perf` 工具通常都是有意义的。

<div id="test-tools-and-scripts">
  ## 测试工具和脚本
</div>

`tests` 目录中的一些程序并不是现成的测试用例，而是测试工具。
例如，对于 `Lexer`，有一个工具 `src/Parsers/tests/lexer`，它仅对 stdin 进行标记化，并将带颜色的结果写入 stdout。
你可以把这类工具用作代码示例，也可以用于探索和手动测试。

<div id="miscellaneous-tests">
  ## 杂项测试
</div>

`tests/external_models` 中有一些针对机器学习模型的测试。
这些测试没有持续更新，必须迁移到集成测试中。

另有一项单独的 quorum 插入测试。
该测试会在不同服务器上运行一个 ClickHouse 集群，并模拟各种故障场景：网络分区、丢包 (例如 ClickHouse 节点之间、ClickHouse 与 ZooKeeper 之间、ClickHouse server 与客户端之间等) 、`kill -9`、`kill -STOP` 和 `kill -CONT`，类似于 [Jepsen](https://aphyr.com/tags/Jepsen)。随后，测试会检查所有已确认的插入都已成功写入，而所有被拒绝的插入都未被写入。

<div id="manual-testing">
  ## 手动测试
</div>

开发新特性时，同时进行手动测试也是合理的。
你可以按以下步骤操作：

构建 ClickHouse。在终端中运行 ClickHouse：切换到 `programs/clickhouse-server` 目录，并执行 `./clickhouse-server`。默认情况下，它会使用当前目录中的配置 (`config.xml`、`users.xml` 以及 `config.d` 和 `users.d` 目录中的文件) 。要连接到 ClickHouse 服务器，请运行 `programs/clickhouse-client/clickhouse-client`。

请注意，所有 clickhouse 工具 (server、client 等) 都只是名为 `clickhouse` 的同一个可执行文件的符号链接。
你可以在 `programs/clickhouse` 找到这个可执行文件。
此外，所有工具也都可以通过 `clickhouse tool` 的形式调用，而不是 `clickhouse-tool`。

或者，你也可以安装 ClickHouse package：既可以使用 ClickHouse 软件源中的稳定版本，也可以在 ClickHouse 源代码根目录中运行 `./release` 自行构建 package。
然后使用 `sudo clickhouse start` 启动 server (或使用 stop 停止 server) 。
日志位于 `/etc/clickhouse-server/clickhouse-server.log`。

如果你的系统中已经安装了 ClickHouse，你可以构建一个新的 `clickhouse` 可执行文件并替换现有的可执行文件：

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

你也可以停止系统中的 clickhouse-server，并使用相同的配置运行你自己的实例，只是将日志输出到终端：

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

gdb 示例：

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

如果系统中的 clickhouse-server 已在运行，而你又不想停止它，可以在 `config.xml` 中修改端口号 (或在 `config.d` 目录中的文件里覆盖这些设置) ，指定合适的数据路径，然后运行它。

`clickhouse` 可执行文件几乎没有依赖项，并且可在多种 Linux 发行版上运行。
如果想在服务器上快速粗略地测试你的改动，只需用 `scp` 将新构建的 `clickhouse` 可执行文件复制到服务器上，然后像上面的示例一样运行即可。

<div id="build-tests">
  ## 构建测试
</div>

构建测试用于检查构建在各种不同配置以及一些其他系统上是否正常。
这些测试也都是自动化的。

示例：

* 为 Darwin x86&#95;64 (macOS) 进行交叉编译
* 为 FreeBSD x86&#95;64 进行交叉编译
* 为 Linux AArch64 进行交叉编译
* 在 Ubuntu 上使用系统软件包中的库进行构建 (不推荐) 
* 以共享方式链接库进行构建 (不推荐) 

例如，使用系统软件包构建并不是一种好的做法，因为我们无法保证某个系统实际安装的软件包版本究竟是什么。
但 Debian 维护者确实需要这种方式。
因此，我们至少必须支持这种构建方式。
再举一个例子：共享链接是常见的问题来源，但对一些爱好者来说这是必需的。

虽然我们无法在所有构建方式上运行全部测试，但我们至少希望检查各种构建方式没有出问题。
为此，我们使用构建测试。

我们还会测试是否存在过长、难以编译或需要过多 RAM 的编译单元。

我们还会测试是否存在过大的栈帧。

<div id="testing-for-protocol-compatibility">
  ## 协议兼容性测试
</div>

当我们扩展 ClickHouse 网络协议时，会手动测试旧版 ClickHouse 客户端是否能与新版 clickhouse-server 正常配合，以及新版 ClickHouse 客户端是否能与旧版 clickhouse-server 正常配合 (只需运行相应软件包中的二进制文件即可) 。

我们也会通过集成测试自动验证一些场景：

* 旧版 ClickHouse 写入的数据能否被新版本成功读取；
* 分布式查询在由不同 ClickHouse 版本组成的集群中能否正常工作。

<div id="help-from-the-compiler">
  ## 编译器的帮助
</div>

ClickHouse 的主要代码 (位于 `src` 目录中) 在构建时会使用 `-Wall -Wextra -Werror`，并额外启用一些警告。
不过，这些选项并不会为第三方库启用。

Clang 还有更多实用的警告——你可以用 `-Weverything` 查看它们，并挑选一些加入默认构建配置。

我们始终使用 clang 构建 ClickHouse，无论是在开发环境还是生产环境中。
你可以在自己的机器上以调试模式构建 (以节省笔记本电脑电量) ，但请注意，由于控制流和过程间分析能力更强，编译器在 `-O3` 下能够生成更多警告。
使用 clang 以调试模式构建时，会使用 `libc++` 的调试版本，从而能在运行时捕获更多错误。

<div id="sanitizers">
  ## Sanitizers
</div>

:::note
如果在本地运行时，进程 (ClickHouse server 或客户端) 在启动时崩溃，你可能需要禁用地址空间布局随机化：`sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Address sanitizer
</div>

我们会在每次提交时都在 ASan 环境下运行功能测试、集成测试、压力测试和单元测试。

<div id="thread-sanitizer">
  ### Thread sanitizer
</div>

我们会对每次提交使用 TSan 运行功能测试、集成测试、压力测试和单元测试。

<div id="memory-sanitizer">
  ### Memory sanitizer
</div>

我们会在每次提交时都使用 MSan 运行功能测试、集成测试、压力测试和单元测试。

<div id="undefined-behaviour-sanitizer">
  ### 未定义行为 sanitizer
</div>

我们会在每次提交后使用 UBSan 运行功能测试、集成测试、压力测试和单元测试。
部分第三方库的代码未启用 UB 检测。

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

我们以前会在夜间通过 Valgrind 运行功能测试，但现在已经不这么做了。
这通常需要好几个小时。
目前已知 `re2` 库中存在一个误报，参见[这篇文章](https://research.swtch.com/sparse)。

<div id="fuzzing">
  ## 模糊测试
</div>

ClickHouse 的模糊测试同时通过 [libFuzzer](https://llvm.org/docs/LibFuzzer.html) 和随机 SQL 查询来实现。
所有模糊测试都应在启用 sanitizer (Address 和 Undefined) 的情况下进行。

libFuzzer 用于对库代码进行隔离式模糊测试。
Fuzzers 作为测试代码的一部分实现，名称以后缀 &quot;&#95;fuzzer&quot; 结尾。
Fuzzer 示例可见于 `src/Parsers/fuzzers/lexer_fuzzer.cpp`。
libFuzzer 专用的配置、字典和语料库存放在 `tests/fuzz` 中。
我们建议你为每个处理用户输入的功能编写模糊测试。

默认情况下不会构建 Fuzzers。
要构建 fuzzers，需要同时设置 `-DENABLE_FUZZING=1` 和 `-DENABLE_TESTS=1` 选项。
我们建议在构建 fuzzers 时禁用 Jemalloc。
用于将 ClickHouse 模糊测试集成到
Google OSS-Fuzz 的配置可在 `docker/fuzz` 中找到。

我们还使用简单的模糊测试来生成随机 SQL 查询，并检查服务端在执行这些查询时不会崩溃。
你可以在 `00746_sql_fuzzy.pl` 中找到它。
此测试应持续运行 (整夜及更长时间) 。

我们还使用了基于 AST 的高级查询 fuzzer，它能够发现大量边界情况。
它会对查询 AST 进行随机置换和替换。
它会记住先前测试中的 AST 节点，并在按随机顺序处理后续测试时将其用于模糊测试。
你可以在[这篇博客文章](https://clickhouse.com/blog/fuzzing-click-house)中进一步了解这个 fuzzer。

<div id="stress-test">
  ## 压力测试
</div>

压力测试是模糊测试的另一种形式。
它会在单台服务器上，以随机顺序并行运行所有功能测试。
测试结果不会被校验。

需要验证以下几点：

* 服务器不会崩溃，也不会触发 Debug 或 sanitizer 陷阱；
* 不会出现死锁；
* 数据库结构保持一致；
* 测试结束后，服务器能够成功停止，并且再次启动时不会出现异常。

共有五种变体 (Debug、ASan、TSan、MSan、UBSan) 。

<div id="thread-fuzzer">
  ## Thread Fuzzer
</div>

Thread Fuzzer (请不要与 Thread Sanitizer 混淆) 是另一种模糊测试，可将线程的执行顺序随机化。
它有助于发现更多边缘情况。

<div id="security-audit">
  ## 安全审计
</div>

我们的安全团队从安全角度对 ClickHouse 进行了初步评估。

<div id="static-analyzers">
  ## 静态分析器
</div>

我们会在每次提交时运行 `clang-tidy`。
同时也启用了 `clang-static-analyzer` 检查。
`clang-tidy` 还用于执行一些风格检查。

我们评估过 `clang-tidy`、`Coverity`、`cppcheck`、`PVS-Studio`、`tscancode` 和 `CodeQL`。
使用说明可在 `tests/instructions/` 目录中找到。

如果你使用 `CLion` 作为 IDE，可以直接利用其中的一些 `clang-tidy` 检查。

我们还使用 `shellcheck` 对 shell 脚本进行静态分析。

<div id="hardening">
  ## 加固
</div>

在调试构建中，我们使用自定义分配器，对用户态内存分配启用 ASLR。

我们还会手动保护那些在分配后应保持为只读的内存区域。

在调试构建中，我们还会使用定制版 libc，以确保不会调用“有害”的函数 (如已过时、不安全或非线程安全的函数) 。

调试断言被广泛使用。

在调试构建中，如果抛出带有“logical error”代码的异常 (这意味着存在 bug) ，程序会提前终止。
这样就可以在发布构建中使用异常，同时在调试构建中将其作为断言处理。

jemalloc 的调试版本用于调试构建。
libc++ 的调试版本用于调试构建。

<div id="runtime-integrity-checks">
  ## 运行时完整性检查
</div>

存储在磁盘上的数据会计算校验和。
MergeTree 表中的数据会同时通过三种方式计算校验和* (压缩数据块、未压缩数据块以及跨块的总校验和) 。
客户端与服务器之间或服务器之间通过网络传输的数据也会计算校验和。
复制可确保各副本上的数据在比特级别完全一致。

这样做是为了防范硬件故障 (存储介质上的位腐化、服务器 RAM 中的比特翻转、网络控制器 RAM 中的比特翻转、网络交换机 RAM 中的比特翻转、客户端 RAM 中的比特翻转，以及线上传输中的比特翻转) 。
请注意，比特翻转很常见，即使使用 ECC RAM 且有 TCP 校验和，也很可能发生 (如果你管理着数千台服务器，并且每天处理 PB 级数据) 。
[观看视频 (俄语) ](https://www.youtube.com/watch?v=ooBAQIe0KlQ)。

ClickHouse 提供了有助于运维工程师发现故障硬件的诊断功能。

* 而且这并不慢。

<div id="code-style">
  ## 代码风格
</div>

代码风格规则见[这里](style.md)。

要检查一些常见的风格问题，可以使用 `utils/check-style` 脚本。

要强制代码符合正确的风格，可以使用 `clang-format`。
`.clang-format` 文件位于源码根目录。
它大体上符合我们当前实际使用的代码风格。
但不建议对现有文件使用 `clang-format`，因为这会让格式变得更糟。
你也可以使用 `clang-format-diff` 工具，可在 clang 源码仓库中找到。

或者，你也可以尝试使用 `uncrustify` 工具来重新格式化代码。
配置文件位于源码根目录下的 `uncrustify.cfg`。
相比 `clang-format`，它经过的测试较少。

`CLion` 有自己的代码格式化工具，需要调整后才能符合我们的代码风格。

<div id="test-coverage">
  ## 测试覆盖率
</div>

我们也会跟踪测试覆盖率，但仅涵盖功能测试，且仅针对 clickhouse-server。
这项工作每天进行。

<div id="tests-for-tests">
  ## 针对测试的测试
</div>

有一项用于检测不稳定测试的自动检查。
它会将所有新增测试运行 100 次 (功能测试) 或 10 次 (集成测试) 。
只要测试有一次失败，就会被视为不稳定。

<div id="test-automation">
  ## 测试自动化
</div>

我们使用 [GitHub Actions](https://github.com/features/actions) 运行测试。

构建作业和测试会在 Sandbox 中按每次提交分别运行。
生成的软件包和测试结果会发布到 GitHub，并可通过直接链接下载。
制品会保留数月。
当你在 GitHub 上提交拉取请求时，我们会为其添加“can be tested”标签，我们的 CI 系统随后会为你构建 ClickHouse 软件包 (如 release、debug、启用 address sanitizer 等) 。