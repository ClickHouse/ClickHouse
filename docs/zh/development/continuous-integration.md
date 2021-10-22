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
