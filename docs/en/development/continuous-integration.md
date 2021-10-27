---
toc_priority: 62
toc_title: 持续集成检查
---

# 持续集成检查

当您提交拉取请求时，ClickHouse 会为您的代码运行一些自动检查[continuous integration (CI) system](tests.md#test-automation).
这发生在存储库维护者（来自 ClickHouse 团队的某个人）筛选了您的代码并将`can be tested`标签添加到您的拉取请求之后。
检查结果列在 GitHub 拉取请求页面上，如[GitHub检查文档](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks)中所述.
如果检查失败，您可能需要修复它。 此页面概述了您可能遇到的检查，以及您可以采取哪些措施来解决这些检查。

如果检查失败看起来与您的更改无关，则可能是一些暂时性故障或基础架构问题。
将空提交推送到拉取请求以重新启动 CI 检查：

```
git reset
git commit --allow-empty
git push
```

如果您不确定该怎么做，请向维护人员寻求帮助。

## 合并到Master

验证 PR 可以合并到 master。 如果没有，它将失败并显示消息'Cannot fetch mergecommit'。
要修复此检查，请按照[GitHub文档](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github)中的说明解决冲突,
或者使用 git 将 `master` 分支合并到你的拉取请求分支。

## 文档检查

尝试构建 ClickHouse 文档网站。 如果您更改了文档中的某些内容，它可能会失败。
最可能的原因是文档中的某些交叉链接是错误的。
转到检查报告并查找`ERROR`和`WARNING`消息。

### 报告详情

- [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
- `docs_output.txt` 包含构建日志。[成功结果示例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)


## 描述检查

检查您的拉取请求的描述是否符合模板 [PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
您必须为您的更改指定一个更改日志类别（例如，错误修复），并编写一条用户可读的消息来描述[CHANGELOG.md](../whats-new/changelog/index.md)的更改


## 推送到 Dockerhub

构建用于构建和测试的 docker 镜像，然后将它们推送到 DockerHub。


## 标记检查

这个检查意味着 CI 系统开始处理拉取请求。 当它处于“待处理”状态时，意味着并非所有检查都已开始。 启动所有检查后，状态更改为“成功”。


## 格式检查

使用 [`utils/check-style/check-style`](https://github.com/ClickHouse/ClickHouse/blob/master/utils/check-style/check -style) 二进制（注意它可以在本地运行）。
如果失败，请按照 [code style guide](style.md) 修复样式错误。


### 报告详情
- [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check.html)
- `output.txt` 包含检查结果错误（无效制表等），空白页表示没有错误。 [成功结果示例](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check/output.txt).


## PVS检查
使用静态分析工具[PVS-studio](https://www.viva64.com/en/pvs-studio/)检查代码。 查看报告以查看确切的错误。 如果可以，请修复它们，如果不能，请向 ClickHouse 维护人员寻求帮助。

### 报告详情
- [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/pvs_check.html)
- `test_run.txt.out.log` 包含构建和分析日志文件。 它仅包括解析或未找到的错误。
- `HTML report` 包含分析结果。 有关其描述，请访问 PVS 的 [官方网站](https://www.viva64.com/en/m/0036/#ID14E9A2B2CD).


## 快速测试
通常这是为 PR 运行的第一次检查。 它构建 ClickHouse 并运行大部分 [无状态功能测试](tests.md#functional-tests)，省略了一些额外操作。
如果失败，则在修复之前不会开始进一步检查。
查看报告以了解哪些测试失败，然后按照[此处](tests.md#functional-test-locally)所述在本地重现失败.

### 报告详情
[状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/fast_test.html)

#### 状态页面文件
- `runlog.out.log` 包含所有的通用日志。
- `test_log.txt`
- `submodule_log.txt` 包含有关克隆和检出所需子模块的消息。
- `stderr.log`
- `stdout.log`
- `clickhouse-server.log`
- `clone_log.txt`
- `install_log.txt`
- `clickhouse-server.err.log`
- `build_log.txt`
- `cmake_log.txt` 包含有关 C/C++ 和 Linux 标志检查的消息。

#### 状态页面列

- *Test name* 包含测试的名称（没有路径，例如所有类型的测试都将被剥离为名称）。
- *测试状态* -- _Skipped_、_Success_ 或_Fail_ 之一。
- *测试时间，秒* - 本次测试为空。


## 构建检查 {#build-check}

各种配置构建 ClickHouse，以用于进一步的步骤。 您必须修复失败的构建。 构建日志通常有足够的信息来修复错误，但您可能必须在本地重现故障。 `cmake` 选项可以在构建日志中找到，搜索 `cmake`。 使用这些选项并遵循 [构建过程](../development/build.md)。

### 报告详情

[状态页示例](https://clickhouse-builds.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/clickhouse_build_check/report.html).

- **Compiler**: `gcc-9` 或 `clang-10`（或其他架构的 `clang-10-xx`，例如 `clang-10-freebsd`）。
- **Build type**: `Debug` or `RelWithDebInfo` (cmake).
- **Sanitizer**: `none` (without sanitizers), `address` (ASan), `memory` (MSan), `undefined` (UBSan), 或 `thread` (TSan).
- **Bundled**: `bundled` 构建使用来自 `contrib` 文件夹的库，`unbundled` 构建使用系统库。
- **Splitted** `splitted` [split build](../development/build.md#split-build)
- **Status**: `success` 或 `fail`
- **Build log**: 构建和文件复制日志，在构建失败时很有用。
- **Build time**.
- **Artifacts**: 构建结果文件（`XXX`是服务器版本，例如`20.8.1.4344`）。
  - `clickhouse-client_XXX_all.deb`
  - `clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
  - `clickhouse-common-staticXXX_amd64.deb`
  - `clickhouse-server_XXX_all.deb`
  - `clickhouse-test_XXX_all.deb`
  - `clickhouse_XXX_amd64.buildinfo`
  - `clickhouse_XXX_amd64.changes`
  - `clickhouse`: 主要构建的二进制文件。
  - `clickhouse-odbc-bridge`
  - `unit_tests_dbms`: 带有 ClickHouse 单元测试的 GoogleTest 二进制文件。
  - `shared_build.tgz`: 使用共享库构建。
  - `performance.tgz`: 用于性能测试的特殊包。


## 特殊构建检查
使用 `clang-tidy` 执行静态分析和代码样式检查。 该报告类似于[构建检查](#build-check)。 修复构建日志中发现的错误。


## 功能无状态测试
在各种配置中构建的 ClickHouse 二进制文件运行 [无状态功能测试](tests.md#functional-tests) -- 发布、调试、使用等。
查看报告以查看哪些测试失败，然后按照 [此处](tests.md#functional-test-locally) 的描述在本地重现失败。
请注意，您必须使用正确的构建配置来重现 -- 在 AddressSanitizer 下测试可能会失败，但在 Debug 中通过。
从 [CI 构建检查页面](../development/build.md#you-dont-have-to-build-clickhouse) 下载二进制文件，或在本地构建它。


## 功能状态测试
运行 [状态功能测试](tests.md#functional-tests)。 与功能无状态测试相同的方式。不同之处在于它们需要来自 [Yandex.Metrica 数据集](../getting-started/example-datasets/metrica.md) 的 `hits` 和 `visits` 表才能运行。


## 集成测试
运行 [integration tests](tests.md#integration-tests).


## 测试流程检查
使用 Testflows 系统运行一些测试。 请参阅[此处](https://github.com/ClickHouse/ClickHouse/tree/master/tests/testflows#running-tests-locally)查看如何在本地运行它们。


## 压力测试
从多个客户端同时运行无状态功能测试以检测与并发相关的错误。
如果失败：

    * 首先修复所有失败；
    * 查看报告以查找服务器日志并检查它们是否可能导致错误。


## 分布式测试

检查[split build](../development/build.md#split-build)配置中的服务器构建可以启动和运行简单查询。
如果失败：

    * 首先修复所有失败；
    * 在本地以 [split build](../development/build.md#split-build) 配置构建服务器并检查它是否可以启动并运行`select 1`。


## 兼容性检查
检查 `clickhouse` 二进制文件是否在具有旧 libc 版本的发行版上运行。 如果失败，请向维护人员寻求帮助。


## AST Fuzzer
运行随机生成的查询以捕获程序错误。 如果失败，请向维护人员寻求帮助。


## 性能测试
测量查询性能的变化。 这是最长的检查，只需不到 6 小时即可运行。 性能测试报告详细描述[这里](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report)。



# QA

> 什么是状态页面上的`Task (private network)`项目？

它是 Yandex 内部工作系统的链接。 Yandex 员工可以看到它的开始时间及其更详细的状态。

> 运行测试的地方

Yandex 内部基础设施的某个地方。
