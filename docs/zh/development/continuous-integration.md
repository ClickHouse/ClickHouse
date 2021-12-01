# 持续集成检查 {#continuous-integration-checks}
当你提交一个pull请求时, ClickHouse[持续集成(CI)系统](https://clickhouse.com/docs/en/development/tests/#test-automation)会对您的代码运行一些自动检查.

这在存储库维护者(来自ClickHouse团队的人)筛选了您的代码并将可测试标签添加到您的pull请求之后发生.

检查的结果被列在[GitHub检查文档](https://docs.github.com/en/github/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/about-status-checks)中所述的GitHub pull请求页面.

如果检查失败，您可能被要求去修复它. 该界面介绍了您可能遇到的检查，以及如何修复它们.

如果检查失败看起来与您的更改无关, 那么它可能是一些暂时的故障或基础设施问题. 向pull请求推一个空的commit以重新启动CI检查:

```
git reset
git commit --allow-empty
git push
```

如果您不确定要做什么，可以向维护人员寻求帮助.

## 与Master合并 {#merge-with-master}
验证PR是否可以合并到master. 如果没有, 它将失败并显示消息'Cannot fetch mergecommit'的.请按[GitHub文档](https://docs.github.com/en/github/collaborating-with-pull-requests/addressing-merge-conflicts/resolving-a-merge-conflict-on-github)中描述的冲突解决, 或使用git将主分支合并到您的pull请求分支来修复这个检查.

## 文档检查 {#docs-check}
尝试构建ClickHouse文档网站. 如果您更改了文档中的某些内容, 它可能会失败. 最可能的原因是文档中的某些交叉链接是错误的. 转到检查报告并查找`ERROR`和`WARNING`消息.

### 报告详情 {#report-details}
-  [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
-  `docs_output.txt`包含构建日志信息. [成功结果案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)

## 描述信息检查 {#description-check}
检查pull请求的描述是否符合[PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md)模板.

您必须为您的更改指定一个更改日志类别(例如，Bug修复), 并且为[CHANGELOG.md](../whats-new/changelog/)编写一条用户可读的消息用来描述更改.

## 推送到DockerHub {#push-to-dockerhub}
生成用于构建和测试的docker映像, 然后将它们推送到DockerHub.

## 标记检查 {#marker-check}
该检查意味着CI系统已经开始处理PR.当它处于'待处理'状态时，意味着尚未开始所有检查. 启动所有检查后，状态更改为'成功'.

# 格式检查 {#style-check}
使用`utils/check-style/check-style`二进制文件执行一些简单的基于正则表达式的代码样式检查(注意, 它可以在本地运行).
如果失败, 按照[代码样式指南](./style.md)修复样式错误.

### 报告详情 {#report-details}
-  [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check.html)
-  `docs_output.txt`记录了查结果错误(无效表格等), 空白页表示没有错误. [成功结果案例](https://clickhouse-test-reports.s3.yandex.net/12550/659c78c7abb56141723af6a81bfae39335aa8cb2/style_check/output.txt)

### PVS 检查 {#pvs-check}
使用静态分析工具[PVS-studio](https://www.viva64.com/en/pvs-studio/)检查代码. 查看报告以查看确切的错误.如果可以则修复它们, 如果不行, 可以向ClickHouse的维护人员寻求帮忙.

### 报告详情 {#report-details}
-  [状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/pvs_check.html)
-  `test_run.txt.out.log`包含构建和分析日志文件.它只包含解析或未找到的错误.
-  `HTML report`包含分析结果.有关说明请访问PVS的[官方网站](https://www.viva64.com/en/m/0036/#ID14E9A2B2CD)

## 快速测试 {#fast-test}
通常情况下这是PR运行的第一个检查.它构建ClickHouse以及大多数无状态运行测试, 其中省略了一些.如果失败，在修复之前不会开始进一步的检查. 查看报告以了解哪些测试失败, 然后按照[此处](./tests.md#functional-test-locally)描述的在本地重现失败.

### 报告详情 {#report-details}
[状态页示例](https://clickhouse-test-reports.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/fast_test.html)

#### 状态页文件 {#status-page-files}
- `runlog.out.log` 是包含所有其他日志的通用日志.
- `test_log.txt`
- `submodule_log.txt` 包含关于克隆和检查所需子模块的消息.
- `stderr.log`
- `stdout.log`
- `clickhouse-server.log`
- `clone_log.txt`
- `install_log.txt`
- `clickhouse-server.err.log`
- `build_log.txt`
- `cmake_log.txt` 包含关于C/C++和Linux标志检查的消息.

#### 状态页列信息 {#status-page-columns}
- 测试名称 -- 包含测试的名称(不带路径, 例如, 所有类型的测试将被剥离到该名称).
- 测试状态 -- 跳过、成功或失败之一.
- 测试时间, 秒. -- 这个测试是空的.

## 建构检查 {#build-check}
在各种配置中构建ClickHouse, 以便在后续步骤中使用. 您必须修复失败的构建.构建日志通常有足够的信息来修复错误, 但是您可能必须在本地重现故障. `cmake`选项可以在构建日志中通过grep `cmake`操作找到.使用这些选项并遵循[一般的构建过程](./build.md).

### 报告详情 {#report-details}
[状态页示例](https://clickhouse-builds.s3.yandex.net/12550/67d716b5cc3987801996c31a67b31bf141bc3486/clickhouse_build_check/report.html)
- **Compiler**: `gcc-9` 或 `clang-10` (或其他架构的`clang-10-xx`, 比如`clang-10-freebsd`).
- **Build type**: `Debug` or `RelWithDebInfo` (cmake).
- **Sanitizer**: `none` (without sanitizers), `address` (ASan), `memory` (MSan), `undefined` (UBSan), or `thread` (TSan).
- **Bundled**: `bundled` 构建使用来自 `contrib` 库, 而 `unbundled` 构建使用系统库.
- **Splitted**: `splitted` is a [split build](https://clickhouse.com/docs/en/development/build/#split-build)
- **Status**: `成功` 或 `失败`
- **Build log**: 链接到构建和文件复制日志, 当构建失败时很有用.
- **Build time**.
- **Artifacts**: 构建结果文件 (`XXX`是服务器版本, 比如`20.8.1.4344`).
    - `clickhouse-client_XXX_all.deb`
    -` clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
    - `clickhouse-common-staticXXX_amd64.deb`
    - `clickhouse-server_XXX_all.deb`
    - `clickhouse-test_XXX_all.deb`
    - `clickhouse_XXX_amd64.buildinfo`
    - `clickhouse_XXX_amd64.changes`
    - `clickhouse`: Main built binary.
    - `clickhouse-odbc-bridge`
    - `unit_tests_dbms`: 带有 ClickHouse 单元测试的 GoogleTest 二进制文件.
    - `shared_build.tgz`: 使用共享库构建.
    - `performance.tgz`: 用于性能测试的特殊包.

## 特殊构建检查 {#special-buildcheck}
使用clang-tidy执行静态分析和代码样式检查. 该报告类似于构建检查. 修复在构建日志中发现的错误.

## 功能无状态测试 {#functional-stateless-tests}
为构建在不同配置中的ClickHouse二进制文件运行[无状态功能测试](./tests.md#functional-tests)——发布、调试、使用杀毒软件等.通过报告查看哪些测试失败，然后按照[此处](./tests.md#functional-test-locally)描述的在本地重现失败.注意, 您必须使用正确的构建配置来重现——在AddressSanitizer下测试可能失败,但在Debug中可以通过.从[CI构建检查页面](./build.md#you-dont-have-to-build-clickhouse)下载二进制文件, 或者在本地构建它.

## 功能有状态测试 {#functional-stateful-tests}
运行[有状态功能测试](./tests.md#functional-tests).以无状态功能测试相同的方式对待它们.不同之处在于它们需要从[Yandex.Metrica数据集](https://clickhouse.com/docs/en/getting-started/example-datasets/metrica/)的`hits`和`visits`表来运行.

## 集成测试 {#integration-tests}
运行[集成测试](./tests.md#integration-tests).

## Testflows 检查{#testflows-check}
使用Testflows测试系统去运行一些测试, 在[此处](https://github.com/ClickHouse/ClickHouse/tree/master/tests/testflows#running-tests-locally)查看如何在本地运行它们.

## 压力测试 {#stress-test}
从多个客户端并发运行无状态功能测试, 用以检测与并发相关的错误.如果失败:
```
* Fix all other test failures first;
* Look at the report to find the server logs and check them for possible causes
  of error.
```

## 冒烟测试 {#split-build-smoke-test}
检查[拆分构建](./build.md#split-build)配置中的服务器构建是否可以启动并运行简单查询.如果失败:
```
* Fix other test errors first;
* Build the server in [split build](./build.md#split-build) configuration
  locally and check whether it can start and run `select 1`.
```

## 兼容性检查 {#compatibility-check}
检查`clickhouse`二进制文件是否可以在带有旧libc版本的发行版上运行.如果失败, 请向维护人员寻求帮助.

## AST模糊器 {#ast-fuzzer}
运行随机生成的查询来捕获程序错误.如果失败, 请向维护人员寻求帮助.

## 性能测试 {#performance-tests}
测量查询性能的变化. 这是最长的检查, 只需不到 6 小时即可运行.性能测试报告在[此处](https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#how-to-read-the-report)有详细描述.

## 质量保证 {#qa}
什么是状态页面上的任务(专用网络)项目?

它是 Yandex 内部工作系统的链接. Yandex 员工可以看到检查的开始时间及其更详细的状态.

运行测试的地方

Yandex 内部基础设施的某个地方.
