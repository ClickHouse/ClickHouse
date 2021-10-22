# 持续集成检查 {#continuous-integration-checks}
当你提交一个pull请求时, ClickHouse的[持续集成(CI)系统](../tests.md#test-automation)会对您的代码运行一些自动检查.

这在存储库维护者(来自ClickHouse团队的人)筛选了您的代码并将可测试标签添加到您的pull请求之后发生.

检查的结果被列在[GitHub检查文档](https://docs.github.com/en/github/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/about-status-checks)中描述的GitHub pull请求页面.

如果检查失败，您可能被要求去修复它. 该界面介绍了您可能遇到的检查，以及如何修复它们.

如果检查失败看起来与您的更改无关，那么它可能是一些暂时的故障或基础设施问题. 向pull请求推一个空的commit以重新启动CI检查:

```
git reset
git commit --allow-empty
git push
```

如果您不确定要做什么，可以向维护人员寻求帮助.
