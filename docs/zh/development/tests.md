# ClickHouse 测试 {#clickhouse-testing}

## 功能测试 {#functional-tests}

功能测试使用起来最简单方便. 大多数 ClickHouse 特性都可以通过功能测试进行测试, 并且对于可以通过功能测试进行测试的 ClickHouse 代码的每一个更改, 都必须使用这些特性

每个功能测试都会向正在运行的 ClickHouse 服务器发送一个或多个查询, 并将结果与参考进行比较.

测试位于 `查询` 目录中. 有两个子目录: `无状态` 和 `有状态`. 无状态测试在没有任何预加载测试数据的情况下运行查询 - 它们通常在测试本身内即时创建小型合成数据集. 状态测试需要来自 Yandex.Metrica 的预加载测试数据, 它对公众开放.

每个测试可以是两种类型之一: `.sql` 和 `.sh`. `.sql` 测试是简单的 SQL 脚本, 它通过管道传输到  `clickhouse-client --multiquery --testmode`. `.sh` 测试是一个自己运行的脚本. SQL 测试通常比 `.sh` 测试更可取. 仅当您必须测试某些无法从纯 SQL 中执行的功能时才应使用 `.sh` 测试, 例如将一些输入数据传送到 `clickhouse-client` 或测试 `clickhouse-local`.

### 在本地运行测试 {#functional-test-locally}

在本地启动ClickHouse服务器, 监听默认端口(9000). 例如, 要运行测试 `01428_hash_set_nan_key`, 请切换到存储库文件夹并运行以下命令:

```
PATH=$PATH:<path to clickhouse-client> tests/clickhouse-test 01428_hash_set_nan_key
```

有关更多选项, 请参阅`tests/clickhouse-test --help`. 您可以简单地运行所有测试或运行由测试名称中的子字符串过滤的测试子集：`./clickhouse-test substring`. 还有并行或随机顺序运行测试的选项.

### 添加新测试 {#adding-new-test}

添加新的测试, 在 `queries/0_stateless` 目录下创建 `.sql` 或 `.sh` 文件, 手动检查, 然后通过以下方式生成`.reference`文件：`clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` 或 `./00000_test.sh > ./00000_test.reference`.

测试应仅使用(创建、删除等)`test` 数据库中假定已预先创建的表; 测试也可以使用临时表.

### 选择测试名称 {#choosing-test-name}

测试名称以五位数前缀开头, 后跟描述性名称, 例如 `00422_hash_function_constexpr.sql`. 要选择前缀, 请找到目录中已存在的最大前缀, 并将其加一. 在此期间, 可能会添加一些具有相同数字前缀的其他测试, 但这没关系并且不会导致任何问题, 您以后不必更改它.

一些测试的名称中标有 `zookeeper`、`shard` 或 `long` . `zookeeper` 用于使用 ZooKeeper 的测试. `shard` 用于需要服务器监听 `127.0.0.*` 的测试; `distributed` 或 `global` 具有相同的含义. `long` 用于运行时间稍长于一秒的测试. Yo你可以分别使用 `--no-zookeeper`、`--no-shard` 和 `--no-long` 选项禁用这些测试组. 如果需要 ZooKeeper 或分布式查询，请确保为您的测试名称添加适当的前缀.

### 检查必须发生的错误 {#checking-error-must-occur}

有时您想测试是否因不正确的查询而发生服务器错误. 我们支持在 SQL 测试中对此进行特殊注释, 形式如下:
```
select x; -- { serverError 49 }
```
此测试确保服务器返回关于未知列“x”的错误代码为 49. 如果没有错误, 或者错误不同, 则测试失败. 如果您想确保错误发生在客户端, 请改用 `clientError` 注释.

不要检查错误消息的特定措辞, 它将来可能会发生变化, 并且测试将不必要地中断. 只检查错误代码. 如果现有的错误代码不足以满足您的需求, 请考虑添加一个新的.

### 测试分布式查询 {#testing-distributed-query}

如果你想在功能测试中使用分布式查询, 你可以使用 `127.0.0.{1..2}` 的地址, 以便服务器查询自己; 或者您可以在服务器配置文件中使用预定义的测试集群, 例如`test_shard_localhost`. 请记住在测试名称中添加 `shard` 或 `distributed` 字样, 以便它以正确的配置在 CI 中运行, 其中服务器配置为支持分布式查询.


## 已知错误 {#known-bugs}

如果我们知道一些可以通过功能测试轻松重现的错误, 我们将准备好的功能测试放在 `tests/queries/bugs` 目录中. 修复错误后, 这些测试将移至 `tests/queries/0_stateless` .

## 集成测试 {#integration-tests}

集成测试允许在集群配置中测试 ClickHouse 以及 ClickHouse 与其他服务器(如 MySQL、Postgres、MongoDB)的交互. 它们可以用来模拟网络分裂、丢包等情况. 这些测试在Docker下运行, 并使用各种软件创建多个容器.

有关如何运行这些测试, 请参阅 `tests/integration/README.md` .

注意, ClickHouse与第三方驱动程序的集成没有经过测试. 另外, 我们目前还没有JDBC和ODBC驱动程序的集成测试.

## 单元测试 {#unit-tests}

当您想测试的不是 ClickHouse 整体, 而是单个独立库或类时，单元测试很有用. 您可以使用 `ENABLE_TESTS` CMake 选项启用或禁用测试构建. 单元测试(和其他测试程序)位于代码中的 `tests` 子目录中. 要运行单元测试, 请键入 `ninja test` 。有些测试使用 `gtest` , 但有些程序在测试失败时会返回非零退出码.

如果代码已经被功能测试覆盖了, 就没有必要进行单元测试(而且功能测试通常更易于使用).

例如, 您可以通过直接调用可执行文件来运行单独的 gtest 检查:

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

## 性能测试 {#performance-tests}

性能测试允许测量和比较 ClickHouse 的某些孤立部分在合成查询上的性能. 测试位于 `tests/performance`. 每个测试都由带有测试用例描述的 `.xml` 文件表示. 测试使用 `docker/tests/performance-comparison` 工具运行. 请参阅自述文件以进行调用.

每个测试在循环中运行一个或多个查询(可能带有参数组合). 一些测试可以包含预加载测试数据集的先决条件.

如果您希望在某些场景中提高ClickHouse的性能，并且如果可以在简单的查询中观察到改进，那么强烈建议编写性能测试。在测试期间使用 `perf top` 或其他perf工具总是有意义的.

## 测试工具和脚本 {#test-tools-and-scripts}

  `tests` 目录中的一些程序不是准备好的测试，而是测试工具. 例如, 对于 `Lexer`, 有一个工具 `src/Parsers/tests/lexer` , 它只是对标准输入进行标记化并将着色结果写入标准输出. 您可以将这些类型的工具用作代码示例以及用于探索和手动测试.

## 其他测试 {#miscellaneous-tests}

在 `tests/external_models` 中有机器学习模型的测试. 这些测试不会更新, 必须转移到集成测试.

仲裁插入有单独的测试. 该测试在不同的服务器上运行 ClickHouse 集群并模拟各种故障情况：网络分裂、丢包(ClickHouse 节点之间、ClickHouse 和 ZooKeeper 之间、ClickHouse 服务器和客户端之间等)、`kill -9`、`kill -STOP` 和 `kill -CONT` , 比如 [Jepsen](https://aphyr.com/tags/Jepsen). 然后测试检查所有已确认的插入是否已写入并且所有被拒绝的插入均未写入.

在 ClickHouse 开源之前, Quorum 测试是由单独的团队编写的. 这个团队不再与ClickHouse合作. 测试碰巧是用Java编写的. 由于这些原因, 必须重写仲裁测试并将其转移到集成测试.

## 手动测试 {#manual-testing}

当您开发一个新特性时, 手动测试它也是合理的. 您可以按照以下步骤进行操作:

构建 ClickHouse. 从终端运行 ClickHouse：将目录更改为 `programs/clickhouse-server` 并使用 `./clickhouse-server` 运行它.  默认情况下, 它将使用当前目录中的配置(`config.xml`、`users.xml` 和`config.d` 和`users.d` 目录中的文件). 要连接到 ClickHouse 服务器, 请运行 `programs/clickhouse-client/clickhouse-client` .

请注意, 所有 clickhouse 工具(服务器、客户端等)都只是指向名为 `clickhouse` 的单个二进制文件的符号链接. 你可以在 `programs/clickhouse` 找到这个二进制文件. 所有工具也可以作为 `clickhouse tool` 而不是 `clickhouse-tool` 调用.

或者, 您可以安装 ClickHouse 包: 从 Yandex 存储库稳定发布, 或者您可以在 ClickHouse 源根目录中使用 `./release` 为自己构建包. 然后使用 `sudo service clickhouse-server start` 启动服务器(或停止以停止服务器). 在 `/etc/clickhouse-server/clickhouse-server.log` 中查找日志.

当您的系统上已经安装了 ClickHouse 时，您可以构建一个新的 `clickhouse` 二进制文件并替换现有的二进制文件:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

您也可以停止系统 clickhouse-server 并使用相同的配置运行您自己的服务器, 但登录到终端:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

使用 gdb 的示例:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

如果系统 clickhouse-server 已经在运行并且你不想停止它, 你可以在你的 `config.xml` 中更改端口号(或在 `config.d` 目录中的文件中覆盖它们), 提供适当的数据路径, 并运行它.

`clickhouse` 二进制文件几乎没有依赖关系, 可以在广泛的 Linux 发行版中使用. 要在服务器上快速而肮脏地测试您的更改, 您可以简单地将新构建的 `clickhouse` 二进制文件 `scp` 到您的服务器, 然后按照上面的示例运行它.

## 测试环境 {#testing-environment}

在发布稳定版之前, 我们将其部署在测试环境中.测试环境是一个集群，处理 [Yandex.Metrica](https://metrica.yandex.com/) 数据的 1/39 部分. 我们与 Yandex.Metrica 团队共享我们的测试环境. ClickHouse无需在现有数据上停机即可升级. 我们首先看到的是, 数据被成功地处理了, 没有滞后于实时, 复制继续工作, Yandex.Metrica 团队没有发现任何问题. 第一次检查可以通过以下方式进行:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

在某些情况下, 我们还会部署到 Yandex 中我们朋友团队的测试环境：Market、Cloud 等. 此外, 我们还有一些用于开发目的的硬件服务器.

## 负载测试 {#load-testing}

部署到测试环境后, 我们使用来自生产集群的查询运行负载测试. 这是手动完成的.

确保您在生产集群上启用了 `query_log`.

收集一天或更长时间的查询日志:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

这是一个复杂的例子. `type = 2` 将过滤成功执行的查询. `query LIKE '%ym:%'` 是从 Yandex.Metrica 中选择相关查询. `is_initial_query` 是只选择客户端发起的查询, 而不是 ClickHouse 本身(作为分布式查询处理的一部分).

`scp` 将此日志记录到您的测试集群并按如下方式运行它:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

(可能你还想指定一个 `--user`)

然后把它留到晚上或周末, 去休息一下.

您应该检查 `clickhouse-server` 没有崩溃, 内存占用是有限的, 且性能不会随着时间的推移而降低.

由于查询和环境的高度可变性, 没有记录和比较精确的查询执行时间.

## 构建测试 {#build-tests}

构建测试允许检查在各种可选配置和一些外部系统上的构建是否被破坏. 这些测试也是自动化的.

示例:
-   Darwin x86_64 (Mac OS X) 交叉编译
-   FreeBSD x86_64 交叉编译
-   Linux AArch64 交叉编译
-   使用系统包中的库在 Ubuntu 上构建（不鼓励）
-   使用库的共享链接构建（不鼓励）

例如, 使用系统包构建是不好的做法, 因为我们无法保证系统将拥有哪个确切版本的包. 但这确实是 Debian 维护者所需要的. 出于这个原因, 我们至少必须支持这种构建变体. 另一个例子: 共享链接是一个常见的麻烦来源, 但对于一些爱好者来说是需要的.

虽然我们无法对所有构建变体运行所有测试, 但我们希望至少检查各种构建变体没有被破坏. 为此, 我们使用构建测试.

我们还测试了那些太长而无法编译或需要太多RAM的没有翻译单元.

我们还测试没有太大的堆栈帧.

## 协议兼容性测试 {#testing-for-protocol-compatibility}

当我们扩展 ClickHouse 网络协议时, 我们手动测试旧的 clickhouse-client 与新的 clickhouse-server 一起工作, 而新的 clickhouse-client 与旧的 clickhouse-server 一起工作(只需从相应的包中运行二进制文件).

我们还使用集成测试自动测试一些案例:
- 旧版本ClickHouse写入的数据是否可以被新版本成功读取;
- 在具有不同 ClickHouse 版本的集群中执行分布式查询.

## 编译器的帮助 {#help-from-the-compiler}

主要的 ClickHouse 代码(位于 `dbms` 目录中)是用 `-Wall -Wextra -Werror` 和一些额外的启用警告构建的. 虽然没有为第三方库启用这些选项.

Clang 有更多有用的警告 - 你可以用 `-Weverything` 寻找它们并选择一些东西来默认构建.

对于生产构建, 使用 clang, 但我们也测试 make gcc 构建. 对于开发, clang 通常使用起来更方便. 您可以使用调试模式在自己的机器上构建(以节省笔记本电脑的电池), 但请注意, 由于更好的控制流和过程间分析, 编译器能够使用 `-O3` 生成更多警告. 在调试模式下使用 clang 构建时, 使用调试版本的 `libc++` 允许在运行时捕获更多错误.

## 地址清理器 {#sanitizers}

### 地址清理器
我们在ASan上运行功能测试、集成测试、压力测试和单元测试.

### 线程清理器
我们在TSan下运行功能测试、集成测试、压力测试和单元测试.

### 内存清理器
我们在MSan上运行功能测试、集成测试、压力测试和单元测试.

### 未定义的行为清理器
我们在UBSan下运行功能测试、集成测试、压力测试和单元测试. 某些第三方库的代码未针对 UB 进行清理.

### Valgrind (Memcheck)
我们曾经在 Valgrind 下通宵运行功能测试, 但不再这样做了. 这需要几个小时. 目前在`re2`库中有一个已知的误报, 见[这篇文章](https://research.swtch.com/sparse).

## 模糊测试 {#fuzzing}

ClickHouse 模糊测试是使用 [libFuzzer](https://llvm.org/docs/LibFuzzer.html) 和随机 SQL 查询实现的. 所有模糊测试都应使用sanitizers(地址和未定义)进行.

LibFuzzer 用于库代码的隔离模糊测试. Fuzzer 作为测试代码的一部分实现, 并具有 `_fuzzer` 名称后缀.
Fuzzer 示例可以在 `src/Parsers/tests/lexer_fuzzer.cpp` 中找到. LibFuzzer 特定的配置、字典和语料库存储在 `tests/fuzz` 中.
我们鼓励您为处理用户输入的每个功能编写模糊测试.

默认情况下不构建模糊器. 要构建模糊器, 应设置` -DENABLE_FUZZING=1` 和 `-DENABLE_TESTS=1` 选项.
我们建议在构建模糊器时禁用 Jemalloc. 用于将 ClickHouse fuzzing 集成到 Google OSS-Fuzz 的配置可以在 `docker/fuzz` 中找到.

我们还使用简单的模糊测试来生成随机SQL查询, 并检查服务器在执行这些查询时是否会死亡.
你可以在 `00746_sql_fuzzy.pl` 中找到它. 这个测试应该连续运行(通宵或更长时间).

我们还使用复杂的基于 AST 的查询模糊器, 它能够找到大量的极端情况. 它在查询 AST 中进行随机排列和替换. 它会记住先前测试中的 AST 节点, 以使用它们对后续测试进行模糊测试, 同时以随机顺序处理它们. 您可以在 [这篇博客文章](https://clickhouse.com/blog/en/2021/fuzzing-clickhouse/) 中了解有关此模糊器的更多信息.

## 压力测试 {#stress-test}

压力测试是另一种模糊测试. 它使用单个服务器以随机顺序并行运行所有功能测试. 不检查测试结果.

经检查:
- 服务器不会崩溃，不会触发调试或清理程序陷阱;
- 没有死锁;
- 数据库结构一致;
- 服务器可以在测试后成功停止并重新启动，没有异常;

有五种变体 (Debug, ASan, TSan, MSan, UBSan).

## 线程模糊器 {#thread-fuzzer}

Thread Fuzzer(请不要与 Thread Sanitizer 混淆)是另一种允许随机化线程执行顺序的模糊测试. 它有助于找到更多特殊情况.

## 安全审计 {#security-audit}

Yandex安全团队的人员从安全的角度对ClickHouse的功能做了一些基本的概述.

## 静态分析仪 {#static-analyzers}

我们在每次提交的基础上运行 `clang-tidy`. `clang-static-analyzer` 检查也被启用. `clang-tidy` 也用于一些样式检查.

我们已经评估了 `clang-tidy`、`Coverity`、`cppcheck`、`PVS-Studio`、`tscancode`、`CodeQL`. 您将在 `tests/instructions/` 目录中找到使用说明. 你也可以阅读[俄文文章](https://habr.com/company/yandex/blog/342018/).

如果你使用 `CLion` 作为 IDE, 你可以利用一些开箱即用的 `clang-tidy` 检查

我们还使用 `shellcheck` 对shell脚本进行静态分析.

## 硬化 {#hardening}

在调试版本中, 我们使用自定义分配器执行用户级分配的 ASLR.

我们还手动保护在分配后预期为只读的内存区域.

在调试构建中, 我们还需要对libc进行自定义, 以确保不会调用 "有害的" (过时的、不安全的、非线程安全的)函数.

Debug 断言被广泛使用.

在调试版本中，如果抛出带有 "逻辑错误" 代码(暗示错误)的异常, 则程序会过早终止. 它允许在发布版本中使用异常, 但在调试版本中使其成为断言.

jemalloc 的调试版本用于调试版本.
libc++ 的调试版本用于调试版本.

## 运行时完整性检查

对存储在磁盘上的数据是校验和. MergeTree 表中的数据同时以三种方式进行校验和*(压缩数据块、未压缩数据块、跨块的总校验和). 客户端和服务器之间或服务器之间通过网络传输的数据也会进行校验和. 复制确保副本上的数据位相同.

需要防止硬件故障(存储介质上的位腐烂、服务器上 RAM 中的位翻转、网络控制器 RAM 中的位翻转、网络交换机 RAM 中的位翻转、客户端 RAM 中的位翻转、线路上的位翻转). 请注意，比特位操作很常见, 即使对于 ECC RAM 和 TCP 校验和(如果您每天设法运行数千台处理 PB 数据的服务器, 也可能发生比特位操作. [观看视频(俄语)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse 提供诊断功能, 可帮助运维工程师找到故障硬件.

\* 它并不慢.

## 代码风格 {#code-style}

[此处](style.md)描述了代码样式规则.

要检查一些常见的样式违规，您可以使用 `utils/check-style` 脚本.

要强制使用正确的代码样式, 您可以使用 `clang-format`. 文件 `.clang-format` 位于源根目录. 它大多与我们的实际代码风格相对应. 但是不建议将 `clang-format` 应用于现有文件, 因为它会使格式变得更糟. 您可以使用可以在 clang 源代码库中找到的 `clang-format-diff` 工具.

或者, 您可以尝试使用 `uncrustify` 工具来重新格式化您的代码. 配置位于源根目录中的 `uncrustify.cfg` 中. 它比 `clang-format` 测试更少.

`CLion` 有自己的代码格式化程序, 必须根据我们的代码风格进行调整.

我们还使用 `codespell` 来查找代码中的拼写错误.它也是自动化的.

## Metrica B2B 测试 {#metrica-b2b-tests}

每个 ClickHouse 版本都使用 Yandex Metrica 和 AppMetrica 引擎进行测试. ClickHouse 的测试版和稳定版部署在 VM 上, 并使用 Metrica 引擎的小副本运行, 该引擎处理输入数据的固定样本. 然后将两个 Metrica 引擎实例的结果放在一起比较.

这些测试由单独的团队自动化. 由于移动部件数量众多, 测试在大多数情况下都因完全不相关的原因而失败, 这些原因很难弄清楚. 这些测试很可能对我们有负面价值. 尽管如此, 这些测试在数百次中被证明是有用的.

## 测试覆盖率 {#test-coverage}

我们还跟踪测试覆盖率, 但仅针对功能测试和 clickhouse-server. 它每天进行.

## Tests for Tests

有自动检测薄片测试. 它运行所有新测试100次(用于功能测试)或10次(用于集成测试). 如果至少有一次测试失败，它就被认为是脆弱的.

## Testflows

[Testflows](https://testflows.com/) 是一个企业级的测试框架. Altinity 使用它进行一些测试, 我们在 CI 中运行这些测试.

## Yandex 检查 (only for Yandex employees)

这些检查将ClickHouse代码导入到Yandex内部的单一存储库中, 所以ClickHouse代码库可以被Yandex的其他产品(YT和YDB)用作库. 请注意, clickhouse-server本身并不是由内部回购构建的, Yandex应用程序使用的是未经修改的开源构建的.

## 测试自动化 {#test-automation}

我们使用 Yandex 内部 CI 和名为 "Sandbox" 的作业自动化系统运行测试.

在每次提交的基础上, 构建作业和测试都在沙箱中运行. 生成的包和测试结果发布在GitHub上, 可以通过直接链接下载. 产物要保存几个月. 当你在GitHub上发送一个pull请求时, 我们会把它标记为 "可以测试" , 我们的CI系统会为你构建ClickHouse包(发布、调试、使用地址清理器等).

由于时间和计算能力的限制, 我们不使用 Travis CI.
我们不用Jenkins. 以前用过, 现在我们很高兴不用Jenkins了.

[原始文章](https://clickhouse.com/docs/en/development/tests/) <!--hide-->
