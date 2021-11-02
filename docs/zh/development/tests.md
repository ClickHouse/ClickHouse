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

Performance tests allow to measure and compare performance of some isolated part of ClickHouse on synthetic queries. Tests are located at `tests/performance`. Each test is represented by `.xml` file with description of test case. Tests are run with `docker/tests/performance-comparison` tool . See the readme file for invocation.

Each test run one or multiple queries (possibly with combinations of parameters) in a loop. Some tests can contain preconditions on preloaded test dataset.

If you want to improve performance of ClickHouse in some scenario, and if improvements can be observed on simple queries, it is highly recommended to write a performance test. It always makes sense to use `perf top` or other perf tools during your tests.

## 测试工具和脚本 {#test-tools-and-scripts}

Some programs in `tests` directory are not prepared tests, but are test tools. For example, for `Lexer` there is a tool `src/Parsers/tests/lexer` that just do tokenization of stdin and writes colorized result to stdout. You can use these kind of tools as a code examples and for exploration and manual testing.

## 其他测试 {#miscellaneous-tests}

There are tests for machine learned models in `tests/external_models`. These tests are not updated and must be transferred to integration tests.

There is separate test for quorum inserts. This test run ClickHouse cluster on separate servers and emulate various failure cases: network split, packet drop (between ClickHouse nodes, between ClickHouse and ZooKeeper, between ClickHouse server and client, etc.), `kill -9`, `kill -STOP` and `kill -CONT` , like [Jepsen](https://aphyr.com/tags/Jepsen). Then the test checks that all acknowledged inserts was written and all rejected inserts was not.

Quorum test was written by separate team before ClickHouse was open-sourced. This team no longer work with ClickHouse. Test was accidentally written in Java. For these reasons, quorum test must be rewritten and moved to integration tests.

## 手动测试 {#manual-testing}

When you develop a new feature, it is reasonable to also test it manually. You can do it with the following steps:

Build ClickHouse. Run ClickHouse from the terminal: change directory to `programs/clickhouse-server` and run it with `./clickhouse-server`. It will use configuration (`config.xml`, `users.xml` and files within `config.d` and `users.d` directories) from the current directory by default. To connect to ClickHouse server, run `programs/clickhouse-client/clickhouse-client`.

Note that all clickhouse tools (server, client, etc) are just symlinks to a single binary named `clickhouse`. You can find this binary at `programs/clickhouse`. All tools can also be invoked as `clickhouse tool` instead of `clickhouse-tool`.

Alternatively you can install ClickHouse package: either stable release from Yandex repository or you can build package for yourself with `./release` in ClickHouse sources root. Then start the server with `sudo service clickhouse-server start` (or stop to stop the server). Look for logs at `/etc/clickhouse-server/clickhouse-server.log`.

When ClickHouse is already installed on your system, you can build a new `clickhouse` binary and replace the existing binary:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

Also you can stop system clickhouse-server and run your own with the same configuration but with logging to terminal:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Example with gdb:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

If the system clickhouse-server is already running and you do not want to stop it, you can change port numbers in your `config.xml` (or override them in a file in `config.d` directory), provide appropriate data path, and run it.

`clickhouse` binary has almost no dependencies and works across wide range of Linux distributions. To quick and dirty test your changes on a server, you can simply `scp` your fresh built `clickhouse` binary to your server and then run it as in examples above.

## 测试环境 {#testing-environment}

Before publishing release as stable we deploy it on testing environment. Testing environment is a cluster that process 1/39 part of [Yandex.Metrica](https://metrica.yandex.com/) data. We share our testing environment with Yandex.Metrica team. ClickHouse is upgraded without downtime on top of existing data. We look at first that data is processed successfully without lagging from realtime, the replication continue to work and there is no issues visible to Yandex.Metrica team. First check can be done in the following way:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

In some cases we also deploy to testing environment of our friend teams in Yandex: Market, Cloud, etc. Also we have some hardware servers that are used for development purposes.

## 负载测试 {#load-testing}

After deploying to testing environment we run load testing with queries from production cluster. This is done manually.

Make sure you have enabled `query_log` on your production cluster.

Collect query log for a day or more:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

This is a way complicated example. `type = 2` will filter queries that are executed successfully. `query LIKE '%ym:%'` is to select relevant queries from Yandex.Metrica. `is_initial_query` is to select only queries that are initiated by client, not by ClickHouse itself (as parts of distributed query processing).

`scp` this log to your testing cluster and run it as following:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

(probably you also want to specify a `--user`)

Then leave it for a night or weekend and go take a rest.

You should check that `clickhouse-server` does not crash, memory footprint is bounded and performance not degrading over time.

Precise query execution timings are not recorded and not compared due to high variability of queries and environment.

## 构建测试 {#build-tests}

Build tests allow to check that build is not broken on various alternative configurations and on some foreign systems. These tests are automated as well.

Examples:
-   cross-compile for Darwin x86_64 (Mac OS X)
-   cross-compile for FreeBSD x86_64
-   cross-compile for Linux AArch64
-   build on Ubuntu with libraries from system packages (discouraged)
-   build with shared linking of libraries (discouraged)

For example, build with system packages is bad practice, because we cannot guarantee what exact version of packages a system will have. But this is really needed by Debian maintainers. For this reason we at least have to support this variant of build. Another example: shared linking is a common source of trouble, but it is needed for some enthusiasts.

Though we cannot run all tests on all variant of builds, we want to check at least that various build variants are not broken. For this purpose we use build tests.

We also test that there are no translation units that are too long to compile or require too much RAM.

We also test that there are no too large stack frames.

## 协议兼容性测试 {#testing-for-protocol-compatibility}

When we extend ClickHouse network protocol, we test manually that old clickhouse-client works with new clickhouse-server and new clickhouse-client works with old clickhouse-server (simply by running binaries from corresponding packages).

We also test some cases automatically with integrational tests:
- if data written by old version of ClickHouse can be successfully read by the new version;
- do distributed queries work in a cluster with different ClickHouse versions.

## 编译器的帮助 {#help-from-the-compiler}

Main ClickHouse code (that is located in `dbms` directory) is built with `-Wall -Wextra -Werror` and with some additional enabled warnings. Although these options are not enabled for third-party libraries.

Clang has even more useful warnings - you can look for them with `-Weverything` and pick something to default build.

For production builds, clang is used, but we also test make gcc builds. For development, clang is usually more convenient to use. You can build on your own machine with debug mode (to save battery of your laptop), but please note that compiler is able to generate more warnings with `-O3` due to better control flow and inter-procedure analysis. When building with clang in debug mode, debug version of `libc++` is used that allows to catch more errors at runtime.

## Sanitizers {#sanitizers}

### Address sanitizer
We run functional, integration, stress and unit tests under ASan on per-commit basis.

### Thread sanitizer
We run functional, integration, stress and unit tests under TSan on per-commit basis.

### Memory sanitizer
We run functional, integration, stress and unit tests under MSan on per-commit basis.

### Undefined behaviour sanitizer
We run functional, integration, stress and unit tests under UBSan on per-commit basis. The code of some third-party libraries is not sanitized for UB.

### Valgrind (Memcheck)
We used to run functional tests under Valgrind overnight, but don't do it anymore. It takes multiple hours. Currently there is one known false positive in `re2` library, see [this article](https://research.swtch.com/sparse).

## 模糊测试 {#fuzzing}

ClickHouse fuzzing is implemented both using [libFuzzer](https://llvm.org/docs/LibFuzzer.html) and random SQL queries.
All the fuzz testing should be performed with sanitizers (Address and Undefined).

LibFuzzer is used for isolated fuzz testing of library code. Fuzzers are implemented as part of test code and have “_fuzzer” name postfixes.
Fuzzer example can be found at `src/Parsers/tests/lexer_fuzzer.cpp`. LibFuzzer-specific configs, dictionaries and corpus are stored at `tests/fuzz`.
We encourage you to write fuzz tests for every functionality that handles user input.

Fuzzers are not built by default. To build fuzzers both `-DENABLE_FUZZING=1` and `-DENABLE_TESTS=1` options should be set.
We recommend to disable Jemalloc while building fuzzers. Configuration used to integrate ClickHouse fuzzing to
Google OSS-Fuzz can be found at `docker/fuzz`.

We also use simple fuzz test to generate random SQL queries and to check that the server does not die executing them.
You can find it in `00746_sql_fuzzy.pl`. This test should be run continuously (overnight and longer).

We also use sophisticated AST-based query fuzzer that is able to find huge amount of corner cases. It does random permutations and substitutions in queries AST. It remembers AST nodes from previous tests to use them for fuzzing of subsequent tests while processing them in random order. You can learn more about this fuzzer in [this blog article](https://clickhouse.com/blog/en/2021/fuzzing-clickhouse/).

## 压力测试 {#stress-test}

Stress tests are another case of fuzzing. It runs all functional tests in parallel in random order with a single server. Results of the tests are not checked.

It is checked that:
- server does not crash, no debug or sanitizer traps are triggered;
- there are no deadlocks;
- the database structure is consistent;
- server can successfully stop after the test and start again without exceptions.

There are five variants (Debug, ASan, TSan, MSan, UBSan).

## 线程模糊器 {#thread-fuzzer}

Thread Fuzzer (please don't mix up with Thread Sanitizer) is another kind of fuzzing that allows to randomize thread order of execution. It helps to find even more special cases.

## 安全审计 {#security-audit}

People from Yandex Security Team do some basic overview of ClickHouse capabilities from the security standpoint.

## 静态分析仪 {#static-analyzers}

We run `clang-tidy` and `PVS-Studio` on per-commit basis. `clang-static-analyzer` checks are also enabled. `clang-tidy` is also used for some style checks.

We have evaluated `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`. You will find instructions for usage in `tests/instructions/` directory. Also you can read [the article in russian](https://habr.com/company/yandex/blog/342018/).

If you use `CLion` as an IDE, you can leverage some `clang-tidy` checks out of the box.

We also use `shellcheck` for static analysis of shell scripts.

## 硬化 {#hardening}

In debug build we are using custom allocator that does ASLR of user-level allocations.

We also manually protect memory regions that are expected to be readonly after allocation.

In debug build we also involve a customization of libc that ensures that no "harmful" (obsolete, insecure, not thread-safe) functions are called.

Debug assertions are used extensively.

In debug build, if exception with "logical error" code (implies a bug) is being thrown, the program is terminated prematurally. It allows to use exceptions in release build but make it an assertion in debug build.

Debug version of jemalloc is used for debug builds.
Debug version of libc++ is used for debug builds.

## 运行时完整性检查

Data stored on disk is checksummed. Data in MergeTree tables is checksummed in three ways simultaneously* (compressed data blocks, uncompressed data blocks, the total checksum across blocks). Data transferred over network between client and server or between servers is also checksummed. Replication ensures bit-identical data on replicas.

It is required to protect from faulty hardware (bit rot on storage media, bit flips in RAM on server, bit flips in RAM of network controller, bit flips in RAM of network switch, bit flips in RAM of client, bit flips on the wire). Note that bit flips are common and likely to occur even for ECC RAM and in presense of TCP checksums (if you manage to run thousands of servers processing petabytes of data each day). [See the video (russian)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse provides diagnostics that will help ops engineers to find faulty hardware.

\* and it is not slow.

## 代码风格 {#code-style}

Code style rules are described [here](style.md).

To check for some common style violations, you can use `utils/check-style` script.

To force proper style of your code, you can use `clang-format`. File `.clang-format` is located at the sources root. It mostly corresponding with our actual code style. But it’s not recommended to apply `clang-format` to existing files because it makes formatting worse. You can use `clang-format-diff` tool that you can find in clang source repository.

Alternatively you can try `uncrustify` tool to reformat your code. Configuration is in `uncrustify.cfg` in the sources root. It is less tested than `clang-format`.

`CLion` has its own code formatter that has to be tuned for our code style.

We also use `codespell` to find typos in code. It is automated as well.

## Metrica B2B 测试 {#metrica-b2b-tests}

Each ClickHouse release is tested with Yandex Metrica and AppMetrica engines. Testing and stable versions of ClickHouse are deployed on VMs and run with a small copy of Metrica engine that is processing fixed sample of input data. Then results of two instances of Metrica engine are compared together.

These tests are automated by separate team. Due to high number of moving parts, tests are fail most of the time by completely unrelated reasons, that are very difficult to figure out. Most likely these tests have negative value for us. Nevertheless these tests was proved to be useful in about one or two times out of hundreds.

## 测试覆盖率 {#test-coverage}

We also track test coverage but only for functional tests and only for clickhouse-server. It is performed on daily basis.

## Tests for Tests

There is automated check for flaky tests. It runs all new tests 100 times (for functional tests) or 10 times (for integration tests). If at least single time the test failed, it is considered flaky.

## Testflows

[Testflows](https://testflows.com/) is an enterprise-grade testing framework. It is used by Altinity for some of the tests and we run these tests in our CI.

## Yandex 检查 (only for Yandex employees)

These checks are importing ClickHouse code into Yandex internal monorepository, so ClickHouse codebase can be used as a library by other products at Yandex (YT and YDB). Note that clickhouse-server itself is not being build from internal repo and unmodified open-source build is used for Yandex applications.

## 测试自动化 {#test-automation}

We run tests with Yandex internal CI and job automation system named “Sandbox”.

Build jobs and tests are run in Sandbox on per commit basis. Resulting packages and test results are published in GitHub and can be downloaded by direct links. Artifacts are stored for several months. When you send a pull request on GitHub, we tag it as “can be tested” and our CI system will build ClickHouse packages (release, debug, with address sanitizer, etc) for you.

We do not use Travis CI due to the limit on time and computational power.
We do not use Jenkins. It was used before and now we are happy we are not using Jenkins.

[原始文章](https://clickhouse.com/docs/en/development/tests/) <!--hide-->
