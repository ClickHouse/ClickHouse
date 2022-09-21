# 如何将测试查询添加到 ClickHouse CI

ClickHouse有数百个（甚至数千个）功能。 每个提交都由包含数千个测试用例的一组复杂测试进行检查。

核心功能经过了很多的测试，但是ClickHouse CI可以发现一些极端情况和不同的功能组合。

我们看到的大多数错误/回归都发生在测试覆盖率较差的`灰色区域`中。

我们非常有兴趣通过测试涵盖实现生活中使用的大多数可能的场景和功能组合。

## 为什么要添加测试

为什么/何时应将测试用例添加到ClickHouse代码中：
1) 您使用了一些复杂的场景/功能组合/您有一些可能未被广泛使用的情况
2) 您会看到更改日志中没有通知的版本之间的某些行为发生了变化
3) 您只是想帮助提高ClickHouse的质量并确保您使用的功能在未来的版本中不会被破坏
4) 一旦测试被添加/接受，您可以确保您检查的角落案例永远不会被意外损坏。
5) 你将成为伟大的开源社区的一份子
6) 您的名字将出现在`system.contributors`表中！
7) 你会让世界变得更好。

### 要做的步骤

#### 先决条件

我假设你运行一些Linux机器（你可以在其他操作系统上使用 docker/虚拟机）和任何现代浏览器/互联网连接，并且你有一些基本的Linux和SQL技能。

不需要任何高度专业化的知识（因此您不需要了解 C++ 或了解ClickHouse CI的工作原理）。

#### 准备

1) [create GitHub account](https://github.com/join) (如果你还没有)
2) [setup git](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/set-up-git)
```bash
# for Ubuntu
sudo apt-get update
sudo apt-get install git

git config --global user.name "John Doe" # fill with your name
git config --global user.email "email@example.com" # fill with your email

```
3) [fork ClickHouse project](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) - 打开 [https://github.com/ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse) and press fork button in the top right corner:
   ![fork repo](https://github-images.s3.amazonaws.com/help/bootcamp/Bootcamp-Fork.png)

4) 例如，将代码fork克隆到PC上的某个文件夹, `~/workspace/ClickHouse`
```
mkdir ~/workspace && cd ~/workspace
git clone https://github.com/< your GitHub username>/ClickHouse
cd ClickHouse
git remote add upstream https://github.com/ClickHouse/ClickHouse
```

#### 测试的新分支

1) 从最新的clickhouse master创建一个新分支
```
cd ~/workspace/ClickHouse
git fetch upstream
git checkout -b name_for_a_branch_with_my_test upstream/master
```

#### 安装并运行 clickhouse

1) 安装`clickhouse-server` (参考[离线文档](https://clickhouse.com/docs/en/getting-started/install/))
2) 安装测试配置（它将使用Zookeeper模拟实现并调整一些设置）
```
cd ~/workspace/ClickHouse/tests/config
sudo ./install.sh
```
3) 运行clickhouse-server
```
sudo systemctl restart clickhouse-server
```

#### 创建测试文件


1) 找到测试的编号 - 在`tests/queries/0_stateless/`中找到编号最大的文件

```sh
$ cd ~/workspace/ClickHouse
$ ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
tests/queries/0_stateless/01520_client_print_query_id.reference
```
目前，测试的最后一个数字是`01520`，所以我的测试将有数字`01521`

2) 使用您测试的功能的下一个编号和名称创建一个SQL文件

```sh
touch tests/queries/0_stateless/01521_dummy_test.sql
```

3) 使用您最喜欢的编辑器编辑SQL文件（请参阅下面的创建测试提示）
```sh
vim tests/queries/0_stateless/01521_dummy_test.sql
```


4) 运行测试，并将其结果放入参考文件中：
```
clickhouse-client -nmT < tests/queries/0_stateless/01521_dummy_test.sql | tee tests/queries/0_stateless/01521_dummy_test.reference
```

5) 确保一切正确，如果测试输出不正确（例如由于某些错误），请使用文本编辑器调整参考文件。

#### 如何创建一个好的测试

- 测试应该是
    - 最小 - 仅创建与测试功能相关的表，删除不相关的列和部分查询
    - 快速 - 不应超过几秒钟（更好的亚秒）
    - 正确 - 失败则功能不起作用
        - 确定性的
    - 隔离/无状态
        - 不要依赖一些环境的东西
        - 尽可能不要依赖时间
- 尝试覆盖极端情况(zeros / Nulls / empty sets / throwing exceptions)
- 要测试该查询返回错误，您可以在查询后添加特殊注释：`-- { serverError 60 }`或`-- { clientError 20 }`
- 不要切换数据库（除非必要）
- 如果需要，您可以在同一节点上创建多个表副本
- 您可以在需要时使用测试集群定义之一（请参阅 system.clusters）
- 使用 `number` / `numbers_mt` / `zeros` / `zeros_mt`和类似的查询要在适用时初始化数据
- 在测试之后和测试之前清理创建的对象（DROP IF EXISTS） - 在有一些脏状态的情况下
- 优先选择同步操作模式 (mutations, merges)
- 以`0_stateless`文件夹中的其他SQL文件为例
- 确保您想要测试的特性/特性组合尚未被现有测试覆盖

#### 测试命名规则

正确地命名测试非常重要，因此可以在clickhouse-test调用中关闭一些测试子集。

| Tester flag| 测试名称中应该包含什么 | 什么时候应该添加标志 |
|---|---|---|---|
| `--[no-]zookeeper`| "zookeeper"或"replica" | 测试使用来自ReplicatedMergeTree家族的表 |
| `--[no-]shard` | "shard"或"distributed"或"global"| 使用到127.0.0.2或类似的连接进行测试 |
| `--[no-]long` | "long"或"deadlock"或"race" | 测试运行时间超过60秒 |

#### Commit / push / 创建PR.

1) commit & push您的修改
```sh
cd ~/workspace/ClickHouse
git add tests/queries/0_stateless/01521_dummy_test.sql
git add tests/queries/0_stateless/01521_dummy_test.reference
git commit # use some nice commit message when possible
git push origin HEAD
```
2) 使用一个在推送过程中显示的链接，创建一个到master的PR
3) 调整PR标题和内容，在`Changelog category (leave one)`中保留
   `Build/Testing/Packaging Improvement`，如果需要，请填写其余字段。
