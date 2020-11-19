# ClickHouse 测试 {#clickhouse-ce-shi}

## 功能性测试 {#gong-neng-xing-ce-shi}

功能性测试是最简便使用的。绝大部分 ClickHouse 的功能可以通过功能性测试来测试，任何代码的更改都必须通过该测试。

每个功能测试会向正在运行的 ClickHouse服务器发送一个或多个查询，并将结果与预期结果进行比较。

测试用例在 `tests/queries` 目录中。这里有两个子目录：`stateless` 和 `stateful`目录。无状态的测试无需预加载测试数据集 - 通常是在测试运行期间动态创建小量的数据集。有状态测试需要来自 Yandex.Metrica 的预加载测试数据，而不向一般公众提供。我们倾向于仅使用«无状态»测试并避免添加新的«有状态»测试。

每个测试用例可以是两种类型之一：`.sql` 和 `.sh`。`.sql` 测试文件是用于管理`clickhouse-client --multiquery --testmode`的简单SQL脚本。`.sh` 测试文件是一个可以自己运行的脚本。

要运行所有测试，请使用 `tests/clickhouse-test` 工具，用 `--help` 可以获取所有的选项列表。您可以简单地运行所有测试或运行测试名称中的子字符串过滤的测试子集：`./clickhouse-test substring`。

调用功能测试最简单的方法是将 `clickhouse-client` 复制到`/usr/bin/`，运行`clickhouse-server`，然后从自己的目录运行`./ clickhouse-test`。

要添加新测试，请在 `tests/queries/0_stateless` 目录内添加新的 `.sql` 或 `.sh` 文件，手动检查，然后按以下方式生成 `.reference` 文件： `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` 或 `./00000_test.sh > ./00000_test.reference`。

测试应该只使用（创建，删除等）`test` 数据库中的表，这些表假定是事先创建的; 测试也可以使用临时表。

如果要在功能测试中使用分布式查询，可以利用 `remote` 表函数和 `127.0.0.{1..2}` 地址为服务器查询自身; 或者您可以在服务器配置文件中使用预定义的测试集群，例如`test_shard_localhost`。

有些测试在名称中标有 `zookeeper`，`shard` 或 `long`。`zookeeper` 用于使用ZooKeeper的测试; `shard` 用于需要服务器监听`127.0.0.*`的测试。`long` 适用于运行时间稍长一秒的测试。

## 已知的bug {#yi-zhi-de-bug}

如果我们知道一些可以通过功能测试轻松复制的错误，我们将准备好的功能测试放在 `tests/queries/bugs` 目录中。当修复错误时，这些测试将被移动到 `tests/queries/0_stateless` 目录中。

## 集成测试 {#ji-cheng-ce-shi}

集成测试允许在集群配置中测试 ClickHouse，并与其他服务器（如MySQL，Postgres，MongoDB）进行 ClickHouse 交互。它们可用于模拟网络拆分，数据包丢弃等。这些测试在Docker下运行，并使用各种软件创建多个容器。

参考 `tests/integration/README.md` 文档关于如何使用集成测试。

请注意，ClickHouse 与第三方驱动程序的集成未经过测试。此外，我们目前还没有与 JDBC 和ODBC 驱动程序进行集成测试。

## 单元测试 {#dan-yuan-ce-shi}

当您想要测试整个 ClickHouse，而不是单个独立的库或类时，单元测试非常有用。您可以使用`ENABLE_TESTS` CMake 选项启用或禁用测试构建。单元测试（和其他测试程序）位于代码中的`tests` 子目录中。要运行单元测试，请键入 `ninja test`。有些测试使用 `gtest`，但有些只是在测试失败时返回非零状态码。

如果代码已经被功能测试覆盖（并且功能测试通常使用起来要简单得多），则不一定要进行单元测试。

## 性能测试 {#xing-neng-ce-shi}

性能测试允许测量和比较综合查询中 ClickHouse 的某些独立部分的性能。测试位于`tests/performance` 目录中。每个测试都由 `.xml` 文件表示，并附有测试用例的描述。使用 `clickhouse performance-test` 工具（嵌入在 `clickhouse` 二进制文件中）运行测试。请参阅 `--help` 以进行调用。

每个测试在循环中运行一个或多个查询（可能带有参数组合），并具有一些停止条件（如«最大执行速度不会在三秒内更改»）并测量一些有关查询性能的指标（如«最大执行速度»））。某些测试可以包含预加载的测试数据集的前提条件。

如果要在某些情况下提高 ClickHouse 的性能，并且如果可以在简单查询上观察到改进，则强烈建议编写性能测试。在测试过程中使用 `perf top` 或其他 perf 工具总是有意义的。

性能测试不是基于每个提交运行的。不收集性能测试结果，我们手动比较它们。

## 测试工具和脚本 {#ce-shi-gong-ju-he-jiao-ben}

`tests`目录中的一些程序不是准备测试，而是测试工具。例如，对于`Lexer`，有一个工具`src/Parsers/tests/lexer` 标准输出。您可以使用这些工具作为代码示例以及探索和手动测试。

您还可以将一对文件 `.sh` 和 `.reference` 与工具放在一些预定义的输入上运行它 - 然后可以将脚本结果与 `.reference` 文件进行比较。这些测试不是自动化的。

## 杂项测试 {#za-xiang-ce-shi}

有一些外部字典的测试位于 `tests/external_dictionaries`，机器学习模型在`tests/external_models`目录。这些测试未更新，必须转移到集成测试。

对于分布式数据的插入，有单独的测试。此测试在单独的服务器上运行 ClickHouse 集群并模拟各种故障情况：网络拆分，数据包丢弃（ClickHouse 节点之间，ClickHouse 和 ZooKeeper之间，ClickHouse 服务器和客户端之间等），进行 `kill -9`，`kill -STOP` 和`kill -CONT` 等操作，类似[Jepsen](https://aphyr.com/tags/Jepsen)。然后，测试检查是否已写入所有已确认的插入，并且所有已拒绝的插入都未写入。

在 ClickHouse 开源之前，分布式测试是由单独的团队编写的，但该团队不再使用 ClickHouse，测试是在 Java 中意外编写的。由于这些原因，必须重写分布式测试并将其移至集成测试。

## 手动测试 {#shou-dong-ce-shi}

当您开发了新的功能，做手动测试也是合理的。可以按照以下步骤来进行：

编译 ClickHouse。在命令行中运行 ClickHouse：进入 `programs/clickhouse-server` 目录并运行 `./clickhouse-server`。它会默认使用当前目录的配置文件 (`config.xml`， `users.xml` 以及在 `config.d` 和 `users.d` 目录的文件)。可以使用 `programs/clickhouse-client/clickhouse-client` 来连接数据库。

或者，您可以安装 ClickHouse 软件包：从 Yandex 存储库中获得稳定版本，或者您可以在ClickHouse源根目录中使用 `./release` 构建自己的软件包。然后使用 `sudo service clickhouse-server start` 启动服务器（或停止服务器）。在 `/etc/clickhouse-server/clickhouse-server.log` 中查找日志。

当您的系统上已经安装了 ClickHouse 时，您可以构建一个新的 `clickhouse` 二进制文件并替换现有的二进制文件：

    sudo service clickhouse-server stop
    sudo cp ./clickhouse /usr/bin/
    sudo service clickhouse-server start

您也可以停止 clickhouse-server 并使用相同的配置运行您自己的服务器，日志打印到终端：

    sudo service clickhouse-server stop
    sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml

使用 gdb 的一个示例:

    sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml

如果 clickhouse-server 已经运行并且您不想停止它，您可以更改 `config.xml` 中的端口号（或在 `config.d` 目录中的文件中覆盖它们），配置适当的数据路径，然后运行它。

`clickhouse` 二进制文件几乎没有依赖关系，适用于各种 Linux 发行版。要快速地测试服务器上的更改，您可以简单地将新建的 `clickhouse` 二进制文件 `scp` 到其他服务器，然后按照上面的示例运行它。

## 测试环境 {#ce-shi-huan-jing}

在将版本发布为稳定之前，我们将其部署在测试环境中测试环境是一个处理\[Yandex.Metrica\]（https://metrica.yandex.com/）总数据的1/39部分大小的集群。我们与 Yandex.Metrica 团队公用我们的测试环境。ClickHouse 在现有数据的基础上无需停机即可升级。我们首先看到数据处理成功而不会实时滞后，复制继续工作，并且 Yandex.Metrica 团队无法看到问题。首先的检查可以通过以下方式完成：

    SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;

在某些情况下，我们还部署到 Yandex 的合作团队的测试环境：市场，云等。此外，我们还有一些用于开发目的的硬件服务器。

## 负载测试 {#fu-zai-ce-shi}

部署到测试环境后，我们使用生产群集中的查询运行负载测试。这是手动完成的。

确保在生产集群中开启了 `query_log` 选项。

收集一天或更多的查询日志：

    clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv

这是一个复杂的例子。`type = 2` 将过滤成功执行的查询。`query LIKE'％ym：％'` 用于从 Yandex.Metrica 中选择相关查询。`is_initial_query` 是仅选择由客户端发起的查询，而不是由 ClickHouse 本身（作为分布式查询处理的一部分）。

`scp` 这份日志到测试机器，并运行以下操作：

    clickhouse benchmark --concurrency 16 < queries.tsv

(可能你需要指定运行的用户 `--user`)

然后离开它一晚或周末休息一下。

你要检查下 `clickhouse-server` 是否崩溃，内存占用是否合理，性能也不会随着时间的推移而降低。

由于查询和环境的高度可变性，不会记录精确的查询执行时序并且不进行比较。

## 编译测试 {#bian-yi-ce-shi}

构建测试允许检查构建在各种替代配置和某些外部系统上是否被破坏。测试位于`ci`目录。它们从 Docker，Vagrant 中的源代码运行构建，有时在 Docker 中运行 `qemu-user-static`。这些测试正在开发中，测试运行不是自动化的。

动机：

通常我们会在 ClickHouse 构建的单个版本上发布并运行所有测试。但是有一些未经过彻底测试的替代构建版本。例子：

-   在 FreeBSD 中的构建；
-   在 Debian 中使用系统包中的库进行构建；
-   使用库的共享链接构建；
-   在 AArch64 平台进行构建。

例如，使用系统包构建是不好的做法，因为我们无法保证系统具有的确切版本的软件包。但 Debian 维护者确实需要这样做。出于这个原因，我们至少必须支持这种构建。另一个例子：共享链接是一个常见的麻烦来源，但是对于一些爱好者来说需要它。

虽然我们无法对所有构建版本运行所有测试，但我们想要检查至少不会破坏各种构建变体。为此，我们使用构建测试。

## 测试协议兼容性 {#ce-shi-xie-yi-jian-rong-xing}

当我们扩展 ClickHouse 网络协议时，我们手动测试旧的 clickhouse-client 与新的 clickhouse-server 和新的clickhouse-client 一起使用旧的 clickhouse-server (只需从相应的包中运行二进制文件)

## 来自编译器的提示 {#lai-zi-bian-yi-qi-de-ti-shi}

ClickHouse 主要的代码 (位于`dbms`目录中) 使用 `-Wall -Wextra -Werror` 构建，并带有一些其他已启用的警告。 虽然没有为第三方库启用这些选项。

Clang 有更多有用的警告 - 您可以使用 `-Weverything` 查找它们并选择默认构建的东西。

对于生产构建，使用 gcc（它仍然生成比 clang 稍高效的代码）。对于开发来说，clang 通常更方便使用。您可以使用调试模式在自己的机器上构建（以节省笔记本电脑的电量），但请注意，由于更好的控制流程和过程分析，编译器使用 `-O3` 会生成更多警告。 当使用 clang 构建时，使用 `libc++` 而不是 `libstdc++`，并且在使用调试模式构建时，使用调试版本的 `libc++`，它允许在运行时捕获更多错误。

## Sanitizers {#sanitizers}

### Address sanitizer
我们使用Asan对每个提交进行功能和集成测试。

### Valgrind (Memcheck)
我们在夜间使用Valgrind进行功能测试。这需要几个小时。目前在 `re2` 库中有一个已知的误报，请参阅[文章](https://research.swtch.com/sparse)。

### Undefined behaviour sanitizer
我们使用Asan对每个提交进行功能和集成测试。

### Thread sanitizer
我们使用TSan对每个提交进行功能测试。目前不使用TSan对每个提交进行集成测试。

### Memory sanitizer
目前我们不使用 MSan。

### Debug allocator
您可以使用 `DEBUG_TCMALLOC` CMake 选项启用 `tcmalloc` 的调试版本。我们在每次提交的基础上使用调试分配器运行测试。

更多请参阅 `tests/instructions/sanitizers.txt`。

## 模糊测试 {#mo-hu-ce-shi}

ClickHouse模糊测试可以通过[libFuzzer](https://llvm.org/docs/LibFuzzer.html)和随机SQL查询实现。
所有的模糊测试都应使用sanitizers（Address及Undefined）。

LibFuzzer用于对库代码进行独立的模糊测试。模糊器作为测试代码的一部分实现，并具有“\_fuzzer”名称后缀。
模糊测试示例在`src/Parsers/tests/lexer_fuzzer.cpp`。LibFuzzer配置、字典及语料库存放在`tests/fuzz`。
我们鼓励您为每个处理用户输入的功能编写模糊测试。

默认情况下不构建模糊器。可通过设置`-DENABLE_FUZZING=1`和`-DENABLE_TESTS=1`来构建模糊器。 我们建议在构建模糊器时关闭Jemalloc。 
用于将ClickHouse模糊测试集成到的Google OSS-Fuzz的配置文件位于`docker/fuzz`。

此外，我们使用简单的模糊测试来生成随机SQL查询并检查服务器是否正常。你可以在`00746_sql_fuzzy.pl` 找到它。测试应连续进行（过夜和更长时间）。

## 安全审计 {#an-quan-shen-ji}

Yandex Cloud 部门的人员从安全角度对 ClickHouse 功能进行了一些基本概述。

## 静态分析 {#jing-tai-fen-xi}

我们偶尔使用静态分析。我们已经评估过 `clang-tidy`， `Coverity`， `cppcheck`， `PVS-Studio`， `tscancode`。您将在 `tests/instructions/` 目录中找到使用说明。你也可以阅读[俄文文章](https://habr.com/company/yandex/blog/342018/).

如果您使用 `CLion` 作为 IDE，您可以开箱即用一些 `clang-tidy` 检查。

## 其他强化 {#qi-ta-qiang-hua}

默认情况下使用 `FORTIFY_SOURCE`。它几乎没用，但在极少数情况下仍然有意义，我们不会禁用它。

## 代码风格 {#dai-ma-feng-ge}

代码风格在[这里](https://clickhouse.tech/docs/en/development/style/) 有说明。

要检查一些常见的样式冲突，您可以使用 `utils/check-style` 脚本。

为了强制你的代码的正确风格，你可以使用 `clang-format` 文件。`.clang-format` 位于源代码根目录， 它主要与我们的实际代码风格对应。但不建议将 `clang-format` 应用于现有文件，因为它会使格式变得更糟。您可以使用 `clang-format-diff` 工具，您可以在 clang 源代码库中找到

或者，您可以尝试`uncrustify` 工具来格式化您的代码。配置文件在源代码的根目录中的`uncrustify.cfg`。它比 `clang-format` 经过更少的测试。

`CLion` 有自己的代码格式化程序，必须调整为我们的代码风格。

## Metrica B2B 测试 {#metrica-b2b-ce-shi}

每个 ClickHouse 版本都经过 Yandex Metrica 和 AppMetrica 引擎的测试。测试和稳定版本的 ClickHouse 部署在虚拟机上，并使用处理输入数据固定样本的度量引擎的小副本运行。将度量引擎的两个实例的结果一起进行比较

这些测试是由单独的团队自动完成的。由于移动部件的数量很多，大部分时间的测试都是完全无关的，很难弄清楚。很可能这些测试对我们来说是负值。然而，这些测试被证明是有用的大约一个或两个倍的数百。

## 测试覆盖率 {#ce-shi-fu-gai-lu}

截至2018年7月，我们不会跟踪测试复盖率。

## 自动化测试 {#zi-dong-hua-ce-shi}

我们使用 Yandex 内部 CI 和名为«沙箱»的作业自动化系统运行测试。我们还继续使用 Jenkins（可在Yandex内部使用）。

构建作业和测试在沙箱中按每次提交的基础上运行。结果包和测试结果发布在 GitHub 上，可以通过直接链接下载，结果会被永久存储。当您在 GitHub 上发送拉取请求时，我们将其标记为«可以测试»，我们的 CI 系统将为您构建 ClickHouse 包（发布，调试，地址消除等）。

由于时间和计算能力的限制，我们不使用 Travis CI。

在 Jenkins，我们运行字典测试，指标B2B测试。我们使用 Jenkins 来准备和发布版本。Jenkins是一种传统的技术，所有的工作将被转移到沙箱中。

[来源文章](https://clickhouse.tech/docs/zh/development/tests/) <!--hide-->
