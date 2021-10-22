# 持续集成检查 {#continuous-integration-checks}
当你提交一个pull请求时, ClickHouse的[持续集成(CI)系统](../tests.md#test-automation)会对您的代码运行一些自动检查.

这在存储库维护者(来自ClickHouse团队的人)筛选了您的代码并将可测试标签添加到您的pull请求之后发生.

检查的结果被列在[GitHub检查文档](https://docs.github.com/en/github/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/about-status-checks)中描述的GitHub pull请求页面.

如果检查失败，您可能被要求去修复它. 该界面介绍了您可能遇到的检查，以及如何修复它们.

如果检查失败看起来与您的更改无关, 那么它可能是一些暂时的故障或基础设施问题. 向pull请求推一个空的commit以重新启动CI检查:

```
git reset
git commit --allow-empty
git push
```

如果您不确定要做什么，可以向维护人员寻求帮助.


# 与Master合并 {#merge-with-master}
验证PR是否可以合并到master. 如果没有, 它将返回消息为'Cannot fetch mergecommit'的失败.要修复这个检查, 解决[GitHub文档](https://docs.github.com/en/github/collaborating-with-pull-requests/addressing-merge-conflicts/resolving-a-merge-conflict-on-github)中描述的冲突, 或者使用git合并主分支到你的pull request分支.

# 文档检查 {#docs-check}
尝试构建ClickHouse文档网站. 如果您更改了文档中的某些内容, 它可能会失败. 最可能的原因是文档中的某些交叉链接是错误的. 转到检查报告并查找`ERROR`和`WARNING`消息.

# 查看报告详情 {#report-details}
-  [状态页案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
-  `docs_output.txt`记录了build日志信息. [成功结果案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)

# 描述信息检查 {#description-check}
检查pull请求的描述是否符合[PULL_REQUEST_TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md)模板.

您必须为您的更改指定一个更改日志类别(例如，Bug修复), 并且为[CHANGELOG.md](https://clickhouse.com/docs/en/whats-new/changelog/)编写一条用户可读的消息用来描述更改.

# 推送到DockerHub {#push-to-dockerhub}
生成用于构建和测试的docker映像, 然后将它们推送到DockerHub.

# 标记检查 {#marker-check}
该检查意味着集成系统已经开始处理PR.当它处于待定状态时, 意味着还没有开始所有的检查. 在所有检查启动后，它将状态更改为'成功'.

# 格式检查 {#style-check}
使用`utils/check-style/check-style`二进制文件执行一些简单的基于正则表达式的代码样式检查(注意, 它可以在本地运行).
如果失败, 按照[代码样式指南](https://clickhouse.com/docs/en/development/style/)修复样式错误.

# 查看报告详情 {#report-details}
-  [状态页案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
-  `docs_output.txt`记录了查结果错误(无效表格等), 空白页表示没有错误. [成功结果案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check/docs_output.txt)

# PVS Check {#pvs-check}
使用静态分析工具[PVS-studio](https://www.viva64.com/en/pvs-studio/)检查代码. 查看报告找出确切的错误.如果可以则修复它们, 如果不行, 可以请ClickHouse的维护人员帮忙.

# 查看报告详情 {#report-details}
-  [状态页案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
-  `test_run.txt.out.log`记录了构建和分析日志文件.它只包含解析或未找到的错误.
-  `HTML report`记录了分析结果.关于它的描述, 请访问PVS的[官方网站](https://www.viva64.com/en/m/0036/#ID14E9A2B2CD)

# {#fast-test}
通常情况下这是PR运行的第一个检查.它构建ClickHouse以及大多数无状态运行测试, 其中省略了一些.如果失败，在修复之前不会开始进一步的检查. 通过报告查看哪些测试失败, 然后按照[这里](https://clickhouse.com/docs/en/development/tests/#functional-test-locally)描述的在本地重现失败.

# 查看报告详情 {#report-details}
[状态页案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)

# 状态页文件 {#status-page-files}
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
- `cmake_log.txt` 包含关于C/ c++和Linux标志检查的消息.

# 状态页列信息 {#status-page-columns}
- 测试名称 -- 包含测试的名称(没有路径, 例如, 所有类型的测试将被剥离到该名称).
- 测试状态 -- 跳过、成功或失败.
- 测试时间, 等等. -- 这个测试是空的.

# 建构检查 {#build-check}
在各种配置中构建ClickHouse, 以便在后续步骤中使用. 您必须修复失败的构建.构建日志通常有足够的信息来修复错误, 但是您可能必须在本地重新生成错误. cmake选项可以在构建日志中找到, 为cmake做准备.使用这些选项并遵循一般的构建过程.

# 查看报告详情 {#report-details}
[状态页案例](https://clickhouse-test-reports.s3.yandex.net/12550/eabcc293eb02214caa6826b7c15f101643f67a6b/docs_check.html)
- **Compiler**: `gcc-9` or `clang-10` (or `clang-10-xx` for other architectures e.g. `clang-10-freebsd`).
- **Build type**: `Debug` or `RelWithDebInfo` (cmake).
- **Sanitizer**: `none` (without sanitizers), `address` (ASan), `memory` (MSan), `undefined` (UBSan), or `thread` (TSan).
- **Bundled**: `bundled` 构建使用来自 `contrib` 库, 而 `unbundled` 构建使用系统库.
- **Splitted**: `splitted` is a [split build](https://clickhouse.com/docs/en/development/build/#split-build)
- **Status**: `成功` 或 `失败`
- **Build log**: 链接到生成和文件复制日志，这在生成失败时很有用.
- **Build time**.
- **Artifacts**: 构建结果文件 (with `XXX` being the server version e.g. `20.8.1.4344`).
    - `clickhouse-client_XXX_all.deb`
    -` clickhouse-common-static-dbg_XXX[+asan, +msan, +ubsan, +tsan]_amd64.deb`
    - `clickhouse-common-staticXXX_amd64.deb`
    - `clickhouse-server_XXX_all.deb`
    - `clickhouse-test_XXX_all.deb`
    - `clickhouse_XXX_amd64.buildinfo`
    - `clickhouse_XXX_amd64.changes`
    - `clickhouse`: Main built binary.
    - `clickhouse-odbc-bridge`
    - `unit_tests_dbms`: GoogleTest二进制和ClickHouse单元测试.
    - `shared_build.tgz`: 使用共享库构建.
    - `performance.tgz`: 用于性能测试的特殊包.

# 特殊的构建检查 {#special-buildcheck}
使用clang-tidy执行静态分析和代码风格检查. 报告类似于构建检查. 修复在构建日志中发现的错误.






