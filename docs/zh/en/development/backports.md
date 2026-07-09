---
description: 'ClickHouse 回移策略和自动化系统概述'
sidebar_label: '回移系统'
sidebar_position: 56
slug: /development/backports
title: '回移系统'
doc_type: 'reference'
---

本文档介绍 ClickHouse 的回移策略，以及用于实施该策略的自动化系统。

<div id="release-model">
  ## 发布模型
</div>

ClickHouse 版本遵循 `YY.M.patch.build-type` 这一命名方案，其中 `YY` 是两位数年份，`M` 是发布月份 (无前导零) ，`patch` 是该 branch 内的 patch 编号，`build` 是单调递增的构建号，`type` 则为 `stable` 或 `lts`。

示例：`25.3.8.23-lts` — 2025 年 3 月的长期支持版，第 8 个 patch，构建号为 23。

有两条发布轨道：

* **稳定版本**大致每月发布一次。最近的三个稳定版本会接收 patch，因此每个版本大约有三个月的活跃支持期。
* **LTS (长期支持) **版本每年在 3 月和 8 月发布。系统会同时支持两个 LTS 版本，且每个版本的支持期至少为 12 个月。

建议在 production 环境中运行 workloads 的用户使用最新的稳定版本或 LTS 发行版，并及时升级到新的 patch 版本，因为 patch 版本不会引入 breaking changes。

<div id="backport-policy">
  ## 回移策略
</div>

并非所有更改都会回移。回移的目标是保持发布分支的稳定性，因此其范围被刻意控制得较窄：

* **安全修复** — 一律回移。
* **严重缺陷修复** (异常 (logical errors) 、数据丢失、结果错误、RBAC 问题) — 按照通用回移规则自动纳入回移范围；此类修复通过 `pr-critical-bugfix` 标签标识，并会自动添加 `pr-must-backport`。
* **稳定性修复和回归修复** — 当变更带来的风险低于保留该缺陷不修复的风险时，会进行回移；此类修复通过由维护者手动添加的 `pr-must-backport` 标识。
* **有可行变通方案的轻微缺陷修复** — 通常不会回移，以避免影响发布分支的稳定性。
* **新功能、改进和性能优化** — 不会回移。

`pr-must-backport` 标签是维护者用于将 PR 标记为需要回移的手动覆盖标记。`pr-critical-bugfix` 标签会触发 CI hook 自动添加 `pr-must-backport` (参见 `pr_labels_and_category.py`) 。

**冲突处理。** 当自动回移无法解决合并冲突时，仍必须创建一个 cherry-pick PR，并将其分配给 original PR 的作者、合并者以及现有受分配人，以便人工解决冲突并完成回移。

<div id="backport-tool">
  ## 回移工具
</div>

上文所述的回移策略由 `tests/ci/cherry_pick.py` 中的自动化工具实现。该工具作为 GitHub Actions 工作流运行在 ClickHouse 基础设施上，涵盖了所有要求：发现活跃的发布分支、筛选符合回移条件的 PR、执行两阶段的 cherry-pick 和回移流程、处理冲突、执行延迟策略，以及保持标签同步。

长期目标是将这一实现提取为一个可供其他项目采用的独立开源 Python 工具。目标设计如下：

* **可配置** — 所有策略参数 (符合条件的标签、延迟窗口、陈旧 PR 阈值、rolling-out 期间的行为等) 都在配置文件中定义，使该工具无需修改代码即可适配任何项目的回移需求。
* **可分发** — 打包为可从 PyPI 安装的自包含 Python wheel，不依赖 ClickHouse 的 CI 基础设施。
* **可编程** — 提供清晰的拉取请求、标签和发布分支对象模型，便于用户在核心引擎之上编写自定义工作流脚本。

<div id="testing">
  ### 测试
</div>

独立工具的规划内容之一，是提供专用的测试套件和轻量级测试基础设施。该基础设施将能够启动临时 GitHub 仓库 (或本地等效环境) ，并预先填充以下内容：

* 一组可配置的分支，用于表示各发布线，
* 带有各种回移标签组合的拉取请求，
* 带有 `release` 标签并指向发布分支的发布 PR。

这样一来，测试就能在真实但可丢弃的仓库上演练完整的自动化流程——标签检测、cherry-pick 分支创建、冲突处理、回移 PR 创建、受理人逻辑、跳过 rolling-out 状态以及延迟策略——而不会影响生产环境状态。同一套基础设施还可在策略变更部署前复用，用于回归测试。

<div id="active-release-branches">
  ## 活跃发布分支
</div>

活跃发布分支是指其对应的发布 PR (带有 `release` 标签) 在 GitHub 上仍处于打开状态的分支。回移自动化会在每次运行时动态发现这些分支，因此当有新版本发布或旧版本进入生命周期终止阶段时，无需修改配置。

发布分支在新版本部署期间可能处于 **rolling-out** 状态 (其发布 PR 带有 `rolling-out` 标签) 。为避免增加滚动发布的复杂性，处于 rolling-out 状态的分支会暂停常规回移。特定版本标签 (例如 `v25.3-must-backport`) 会覆盖这一规则，即使在滚动发布期间也会强制执行回移。

特定版本标签指定了该 PR 必须到达的*最早*发布版本：它会被回移到该版本**以及之后每一个更新的活跃发布分支**，而不只是标签中点名的那个版本。例如，若一个已合并到开发分支的 PR 带有 `v25.3-must-backport`，则会回移到 `25.3` 以及其后的每个活跃发布版本 (`25.4`、`25.5`、……) 。如果同时存在多个特定版本标签，则以最低版本为准，因为它已经涵盖了后续更新的版本。

标签中点名的发布版本本身不一定必须是活跃的。针对已终止生命周期版本的标签 (即没有打开的发布 PR 的版本) ，仍会将修复继续带到其后的每个活跃发布版本中，因此从该版本升级时，不会在无提示的情况下丢失这一修复。例如，PR 上的 `v25.12-must-backport` 即使在 `25.12` 本身已终止生命周期后，仍会继续回移到 `26.1`、`26.2`、……。

<div id="implementation">
  ## 实施
</div>

<div id="overview">
  ### 概览
</div>

回移自动化作为 `CherryPick` GitHub Actions 工作流 (`.github/workflows/cherry_pick.yml`) 每小时运行一次，其实现位于 `tests/ci/cherry_pick.py`。它通过 GitHub API 以及在自托管的 `style-checker-aarch64` runner 上执行的本地 git 操作运行。

对于每一组 (original PR、release branch) ，该流程分为两个阶段：

1. 创建一个 **cherry-pick PR**，用于将冲突解决与实际 merge 目标分离。如果没有冲突，则会自动合并。
2. 针对实际的 release branch 创建一个 **backport PR**，并将 cherry-pick 的更改 squashed 成一个 commit。

<div id="labels">
  ### 标签
</div>

原始 PR 上的标签用于控制是否需要回移，以及回移到哪些位置。

| 标签                                                 | 作用                                                                                                                                    |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `pr-must-backport`                                 | 回移到所有活跃的发布分支 (跳过标记了 `rolling-out` 的分支)                                                                                                |
| `pr-must-backport-force`                           | 回移到所有活跃的发布分支，忽略 `rolling-out` 限制                                                                                                      |
| `pr-critical-bugfix`                               | 自动触发 `pr-must-backport` (通过 `pr_labels_and_category.py` 中的 `AUTO_BACKPORT`)                                                           |
| `v{VER}-must-backport` (例如 `v25.3-must-backport`)  | 回移到该发布分支**及所有更新的活跃发布分支**——该版本号表示此 PR 必须到达的*最旧*发布版本，即使该命名发布版本本身已经 end-of-life 也是如此。若存在多个此类标签，以最低版本为准。对于这些分支，此标签会覆盖 `rolling-out` 的跳过规则 |
| `pr-backports-created`                             | 当所有必需的回移 PR 都已创建时，由 bot 设置；如果某个 cherry-pick PR 被重新打开，则会清除                                                                             |
| `pr-cherrypick`                                    | 用于 bot 创建的 cherry-pick PR                                                                                                             |
| `pr-backport`                                      | 用于 bot 创建的回移 PR                                                                                                                       |
| `do not test`                                      | 用于 cherry-pick PR，以便 CI 不在其上运行                                                                                                        |
| `rolling-out`                                      | 设置在 **release PR** 上，表示其分支当前正在滚动发布；常规回移会跳过该分支                                                                                         |

<div id="branch-and-pr-naming">
  ### 分支和 PR 命名
</div>

对于每个原始 PR 编号 `N` 和发布分支 `release/X.Y`：

* Cherry-pick 分支：`cherrypick/release/X.Y/N`
* backport 分支：`backport/release/X.Y/N`
* Cherry-pick PR 标题：`Cherry pick #N to release/X.Y: <original title>`
* 回移 PR 标题：`Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### 逐步操作流程
</div>

<div id="discover-active-releases">
  #### 1. 发现当前活跃的发行版
</div>

`BackportPRs.receive_release_prs` 会在 GitHub 中查询所有带有 `release` 标签且处于打开状态的 PR。这些 PR 的 head ref 就是发布分支名称 (例如 `release/25.3`) 。据此，它会推导出需要搜索的特定版本标签集合：仓库中所有存在的 `v{VER}-must-backport` 标签，只要其版本不高于最新的活跃发行版，都会纳入搜索范围。即使某个标签对应的发行版已不再活跃，较早的标签仍会被包含在内 (比所有活跃发行版都新的标签会被跳过，因为它无法扩展到任何活跃分支) ，因此，只要仍有较新的发行版处于活跃状态，被标记为 end-of-life 发行版的 PR 也依然能够被找到。

<div id="find-prs-to-backport">
  #### 2. 查找需要回移的 PR
</div>

`BackportPRs.receive_prs_for_backport` 使用 GitHub 搜索 API 查找满足以下条件的已合并 PR：

* 带有至少一个回移标签 (`pr-must-backport`、`pr-must-backport-force`、`pr-critical-bugfix` 或特定版本标签) ，且
* **不**带有 `pr-backports-created`，且
* 合并时间晚于任一发布分支上找到的最早提交日期，且
* 在最近 90 天内有更新 (以提高搜索查询效率) 。

<div id="rolling-out-branch-handling">
  #### 3. `rolling-out` 分支处理
</div>

当某个发布 PR 带有 `rolling-out` 标签时，通用回移标签 (`pr-must-backport`、`pr-critical-bugfix`) 会跳过该分支。bot 会关闭此前为该分支创建的所有 cherry-pick 或 回移 PR，并附上说明性注释。特定版本标签 (例如 `v25.3-must-backport`) 始终会覆盖这一行为——对指定的发布版本以及它扩展到的每个较新的活动发布分支都是如此。`pr-must-backport-force` 会对所有分支绕过 `rolling-out` 检查。

<div id="cherry-pick-stage">
  #### 4. Cherry-pick 阶段 (`ReleaseBranch.create_cherrypick`)
</div>

对于每一组尚未存在 cherry-pick PR 的 (原始 PR、发布分支) 组合：

1. 检出发布分支，并基于它创建一个 **backport 分支** (`backport/release/X.Y/N`) 。
2. 对合并提交的第一个父提交执行 `git merge -s ours`，以创建一个不包含内容变更的合成合并基线。
3. 强制创建一个直接指向原始 PR 的合并提交的 **cherry-pick 分支** (`cherrypick/release/X.Y/N`) 。
4. 尝试通过 `git merge --no-commit --no-ff` 将 cherry-pick 分支合并到 backport 分支：
   * 如果已经是最新状态，说明该变更已存在于发布分支中——标记为完成并跳过。
   * 否则 (无论是否有冲突) ，都重置并推送这两个分支。
5. 创建 cherry-pick PR，来源分支为 `cherrypick/release/X.Y/N`，目标分支为 `backport/release/X.Y/N`，并添加 `pr-cherrypick` 和 `do not test` 标签。
6. 如适用，从原始 PR 继承 `pr-bugfix` 或 `pr-critical-bugfix` 标签。
7. 此时**不**设置受分配人；只有在检测到冲突时才会添加。

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. 无冲突 cherry-pick PR 的自动合并
</div>

如果 cherry-pick PR 可合并 (即无冲突) ，机器人会通过 GitHub API 自动将其合并，并立即进入回移阶段。

<div id="backport-stage">
  #### 6. 回移阶段 (`ReleaseBranch.create_backport`)
</div>

在 cherry-pick PR 合并后：

1. 检出并拉取 backport 分支。
2. 找到发布分支与 backport 分支之间的 merge-base。
3. 对该 merge-base 执行 `git reset --soft`，将所有 cherry-pick 的提交压缩为一个提交。
4. 提交时使用 回移 PR 的标题作为提交消息。
5. 强制推送 backport 分支，并创建一个以实际发布分支为目标分支的 回移 PR。
6. 为 PR 添加 `pr-backport` 标签 (如适用，也添加 `pr-bugfix` / `pr-critical-bugfix`) 。
7. 将 PR 分配给原始 PR 的作者、合并者以及现有受分配人 (不包括机器人账户) 。

<div id="completion">
  #### 7. 完成
</div>

当某个原始 PR 对应的所有发布分支都已完成回移后，机器人会在该原始 PR 上添加 `pr-backports-created`。

<div id="pre-check">
  #### 8. 预检查
</div>

在开始处理任何 PR 之前，`ReleaseBranch.pre_check` 会运行 `git merge-base --is-ancestor`，以确认该合并提交是否尚未包含在发布分支中。如果已经包含，则该 PR 会被视为已回移并跳过。

<div id="stale-cherry-pick-pr-handling">
  ### 长期未更新的 Cherry-pick PR 处理
</div>

`CherryPickPRs` 类会在每小时执行开始时运行，并处理以下两种情况：

* **孤立的 cherry-pick PR**：如果某个 cherry-pick PR 对应的发布分支不再有处于打开状态的发布 PR (即该 release 已关闭) ，则该 cherry-pick PR 会被自动关闭。
* **重新打开的 cherry-pick PR**：如果某个原始 PR 已带有 `pr-backports-created`，但对应的 cherry-pick PR 仍处于打开状态，则会从原始 PR 中移除 `pr-backports-created` 标签，以便重新处理。

对于等待手动解决冲突的 cherry-pick PR：

* 在 **3 天**没有更新后，机器人会发布一条 ping 评论，并提及受分配人。
* 在 **7 天**没有更新后，机器人会发布一条关闭评论并关闭该 PR。

<div id="conflict-resolution">
  ### 冲突处理
</div>

当 cherry-pick 出现冲突时，cherry-pick PR 会保持打开状态，等待人工处理。机器人会将其分配给原始 PR 的作者、合并者和受分配人。冲突解决后，一旦 cherry-pick PR 被合并，机器人会在下一次每小时运行时创建回移 PR。

如果要彻底放弃某次回移，请关闭 cherry-pick PR。机器人会将其视为有意跳过。

要从头重新创建一个损坏的 cherry-pick PR：

1. 从 cherry-pick PR 中移除 `pr-cherrypick` 标签。
2. 删除 `cherrypick/...` 分支。
3. 如果存在，从原始 PR 中移除 `pr-backports-created`。

<div id="ci-for-backport-prs">
  ### 回移 PR 的 CI
</div>

回移 PR 以发布分支为目标，因此使用的是专用 CI 工作流 (`BackportPR`，定义在 `ci/workflows/backport_branches.py` 中) ，而不是标准的拉取请求工作流。该工作流会运行一组有代表性的 CI 检查：ASan/UBSan 和 TSan 构建、release 构建、macOS 构建、在 ASan 下运行的功能测试、在 TSan 下运行的压力测试，以及集成测试。它还会验证 backport 分支是否包含 1 到 50 个提交，且至少有一个发生变更的文件 (由 `check_backport_branch.py` 强制检查) 。

<div id="authentication">
  ### 身份验证
</div>

该工作流使用 SSH 密钥 (`ROBOT_CLICKHOUSE_SSH_KEY`) 执行 git push 操作。GitHub API 调用则通过 `get_best_robot_token` 进行身份验证；该函数会从存储在 SSM (`/github-tokens`) 中的令牌池里，选择剩余配额最多的令牌。`ROBOT_CLICKHOUSE_COMMIT_TOKEN` 用于 GitHub Actions 工作流中的 checkout 步骤，不用于 API 调用。分配负责人时，会排除机器人账户 (`robot-clickhouse`、`clickhouse-gh`) 。

<div id="github-api-cache">
  ### GitHub API 缓存
</div>

`GitHubCache` (来自 `cache_utils.py`) 会将 PyGithub 的对象缓存持久化到 S3，以减少每小时运行时的 API 调用次数。缓存会在每次运行开始时下载，并在结束时上传。

<div id="error-handling">
  ### 错误处理
</div>

单个 PR 处理过程中出现的错误会被捕获并记录到日志中，但不会中止本次运行。在所有 PR 都处理完成后，如果期间发生过任何错误，则会引发 `BackportException`。在 CI 中，这会通过 `CIBuddy` 触发向团队群聊发送通知。