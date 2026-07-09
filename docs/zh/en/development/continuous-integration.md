---
description: 'ClickHouse 持续集成系统概览'
sidebar_label: '持续集成（CI）'
sidebar_position: 55
slug: /development/continuous-integration
title: '持续集成（CI）'
doc_type: 'reference'
---

当你提交拉取请求时，ClickHouse 的[持续集成 (CI) 系统](tests.md#test-automation)会对你的代码运行一些自动检查。
这会在仓库维护者 (ClickHouse 团队成员) 审核你的代码，并为你的拉取请求添加 `can be tested` label 之后发生。
如 [GitHub checks 文档](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks)所述，这些检查结果会列在 GitHub 拉取请求页面上。
如果某项检查失败，你可能需要修复它。
本页概述了你可能遇到的各类检查，以及可采取的修复方法。

如果看起来检查失败与你的更改无关，则可能只是暂时性故障或基础设施问题。
向该拉取请求 Push 一个空 commit，以重新启动 CI 检查：

```shell
git commit --allow-empty
git push
```

如不确定如何操作，请向维护者寻求帮助。

<div id="merge-with-master">
  ## 与 master 合并
</div>

验证 PR 是否可合并到 master。
如果不能，检查将失败，并显示消息 `Cannot fetch mergecommit`。
要修复此检查，请按照 [GitHub 文档](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github) 中的说明解决冲突，或使用 git 将 `master` 分支合并到你的拉取请求分支。

<div id="docs-check">
  ## 文档校验
</div>

尝试构建 ClickHouse 文档网站。
如果你修改了文档中的某些内容，此项检查可能会失败。
最可能的原因是文档中的某个交叉引用链接有误。
前往检查报告，查找 `ERROR` 和 `WARNING` 消息。

<div id="description-check">
  ## 描述检查
</div>

检查你的拉取请求描述是否符合模板 [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md)。
你必须为本次更改指定一个更新日志类别 (例如“缺陷修复”) ，并为 [CHANGELOG.md](../whats-new/changelog/index.md) 编写一条用户可读的变更说明。

<div id="docker-image">
  ## Docker 镜像
</div>

构建 ClickHouse server 和 Keeper 的 Docker 镜像，以验证其能否正确构建。

<div id="official-docker-library-tests">
  ### 官方 Docker 库测试
</div>

运行[官方 Docker 库](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files)中的测试，以验证 `clickhouse/clickhouse-server` Docker 镜像能否正常工作。

要添加新测试，请创建目录 `ci/jobs/scripts/docker_server/tests/$test_name`，并在其中创建脚本 `run.sh`。

有关这些测试的更多信息，请参阅 [CI 作业脚本文档](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server)。

<div id="marker-check">
  ## 标记检查
</div>

此检查表示 CI 系统已开始处理该拉取请求。
当其状态为 &#39;pending&#39; 时，表示尚未启动所有检查。
待所有检查都启动后，其状态会变为 &#39;success&#39;。

<div id="style-check">
  ## 风格检查
</div>

对代码库执行各类风格检查。下面的每个子检查都对应 [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py) 中的一个 `testname`，并且都可以通过 `--test <name>` 单独运行 (见下文) 。

<div id="cpp">
  ##### cpp
</div>

通过 [`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh) 进行基于 Regex 的 C++ 风格检查。若检查失败，请根据[代码风格指南](style.md)修复相关问题。

<div id="whitespace-check">
  ##### whitespace_check
</div>

标记 C++ 中逗号后的双空格，但不包括用于列对齐的情况。

<div id="catch-all">
  ##### catch_all
</div>

禁止在析构函数、`main` 和 fuzzer 入口点之外使用 `catch (...)`，因为在这些位置吞掉未知异常并不安全。

<div id="yamllint">
  ##### yamllint
</div>

使用 `.yamllint` 检查 `.github/` 下的 YAML 工作流文件。

<div id="xmllint">
  ##### xmllint
</div>

用于验证 `tests/` 和 `programs/` 目录下的 XML 文件。

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

检查无状态测试：对 `event_date` 进行过滤的查询必须使用 `>= yesterday()`，而不能使用 `today()` (以避免午夜前后出现偶发性不稳定) ，并且测试文件名不得包含 `fail`。

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

标记无状态测试编号中较大的空缺 (`tests/queries/0_stateless/<NNNNN>_*`) 。

<div id="symlinks">
  ##### 符号链接
</div>

检测仓库中失效的符号链接。

<div id="various">
  ##### 各项检查
</div>

通过 [`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh) 执行的杂项仓库检查包括：对 `system.query_log` / `system.parts` / 等的查询必须按 `currentDatabase` 过滤；`Replicated*MergeTree` 的 ZooKeeper 路径必须包含每个测试独有的前缀；集成测试目录必须包含 `__init__.py`；不得存在 UTF BOM；源代码/数据文件不得设置可执行位；第三方 docker-compose 镜像不得使用 `:latest` 标签；等等。

<div id="running-style-check-locally">
  ### 在本地运行风格检查任务
</div>

整个 *风格检查* 任务都可以通过以下方式在 Docker 容器中本地运行：

```sh
python -m ci.praktika run "Style check"
```

要运行特定的检查 (例如 *cpp* 检查) ：

```sh
python -m ci.praktika run "Style check" --test cpp
```

这些命令会拉取 `clickhouse/style-test` Docker 镜像，并在容器化环境中运行该任务。
除 Python 3 和 Docker 外，无需其他依赖项。

<div id="running-stateless-tests">
  ## 运行无状态测试
</div>

在本地安装并使用默认设置的 ClickHouse 可能适用于某些特定测试用例，但无法正确运行测试中的所有查询。在 CI 中，每个任务都会安装特定的 ClickHouse 配置 (例如 S3 存储、并行副本) ，手动复现这些配置可能会很繁琐。为避免这一问题，你可以在本地使用与 CI 相同的编排方式复现任意 CI 任务——无需手动配置。

<div id="ci-prerequisites">
  #### 前置条件
</div>

* Python 3 (仅使用标准库)
* Docker

如有需要，请在 Ubuntu 上安装 Docker，然后重新登录：

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

<div id="run-ci-job-locally">
  #### 在本地运行 CI 任务
</div>

从 CI 报告中任选一个任务名称，然后在本地运行：

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* 务必按 CI 报告中的原样准确引用作业名称 (其中可能包含空格和逗号) ，例如：`"Stateless tests (amd_debug, parallel)"`。这样会使用与 CI 相同的 ClickHouse 配置，并运行相同的测试。
* 作业名称中的架构和构建类型 (例如 `amd_debug`) 是 CI 特有的标记。在本地运行时，它们不会产生任何影响——作业会使用你提供的二进制文件，以及你当前所运行的架构。作业名称只决定 ClickHouse 配置和测试集 (除非通过 `--test` 覆盖) 。
* 在 CI 中，功能测试 会被拆分成多个批次，以更好地利用资源。例如，`"Stateless tests (amd_debug, parallel)"` 和 `"Stateless tests (amd_debug, sequential)"` 合起来覆盖完整范围：可安全并行的测试会并发运行，其余测试则按顺序运行。这种拆分会在可能的情况下尽量提高并行度，从而缩短 CI 总耗时。若要在本地复现完整的测试范围，请运行这两个批次。
* 此外，还有一个 `"Fast test"` CI 作业，会运行范围有限的 功能测试 以验证 ClickHouse 的基础功能——它使用的是不含所有可选模块的构建版本，也是发现回归问题最快的方式。你也可以用同样的方法在本地运行它。请将你的 ClickHouse 二进制文件放到默认搜索路径之一 (`./ci/tmp/clickhouse`、`./build/programs/clickhouse` 或 `./clickhouse`) ——否则该作业会先尝试构建 ClickHouse：
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### 在 CI 任务中运行特定测试
</div>

使用 `--test` 时，该任务会准备与 CI 中使用的 ClickHouse 配置完全相同的环境，但只运行选定的测试：

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* 你可以传入多个测试名称：
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* 提示：如果任意 ClickHouse 配置都可以，只是需要运行特定测试，请使用别名 `functional`，而不要使用完整的作业名称：
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### 其他自定义选项
</div>

* `--path PATH` — ClickHouse 二进制文件的自定义路径。默认情况下，运行器会按以下顺序查找：`./ci/tmp/clickhouse`、`./build/programs/clickhouse`、`./clickhouse`。
* `--count N` — 每个测试重复执行 N 次。
* `--workers N` — 覆盖根据机器容量自动计算出的并行工作线程数。

<div id="build-check">
  ## 构建检查
</div>

以多种配置构建 ClickHouse，供后续步骤使用。

<div id="running-builds-locally">
  ### 在本地运行构建
</div>

可以通过以下方式在本地的类 CI 环境中运行构建：

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

除 Python 3 和 Docker 外，无需其他依赖项。

<div id="available-build-jobs">
  #### 可用的构建作业
</div>

构建作业名称与 CI 报告中的显示名称完全一致：

**AMD64 构建：**

* `Build (amd_debug)` - 带符号的 Debug 构建
* `Build (amd_release)` - 优化后的发布构建
* `Build (amd_asan)` - Address Sanitizer 构建
* `Build (amd_tsan)` - Thread Sanitizer 构建
* `Build (amd_msan)` - Memory Sanitizer 构建
* `Build (amd_ubsan)` - Undefined Behavior Sanitizer 构建
* `Build (amd_binary)` - 不使用 Thin LTO 的快速发布构建
* `Build (amd_compat)` - 面向旧系统的兼容性构建
* `Build (amd_musl)` - 使用 musl libc 的构建
* `Build (amd_darwin)` - macOS 构建
* `Build (amd_freebsd)` - FreeBSD 构建

**ARM64 构建：**

* `Build (arm_release)` - ARM64 优化发布构建
* `Build (arm_asan)` - ARM64 Address Sanitizer 构建
* `Build (arm_coverage)` - 启用覆盖率插桩的 ARM64 构建
* `Build (arm_binary)` - 不使用 Thin LTO 的 ARM64 快速发布构建
* `Build (arm_darwin)` - macOS ARM64 构建
* `Build (arm_v80compat)` - ARMv8.0 兼容性构建

**其他架构：**

* `Build (ppc64le)` - PowerPC 64 位小端
* `Build (riscv64)` - RISC-V 64 位
* `Build (s390x)` - IBM System/390 64 位
* `Build (loongarch64)` - LoongArch 64 位

如果作业成功，构建结果将位于 `<repo_root>/ci/tmp/build` 目录中。

**注意：** 对于不属于“其他架构”类别的构建 (“其他架构”类别使用交叉编译) ，你的本地机器架构必须与构建类型一致，才能按 `BUILD_JOB_NAME` 的要求生成相应构建。

<div id="example-run-local">
  #### 示例
</div>

若要运行本地调试构建：

```bash
python -m ci.praktika run "Build (amd_debug)"
```

如果上述方法不适用，请使用构建日志中的 cmake 选项，并按照[通用构建流程](../development/build.md)进行操作。

<div id="functional-stateless-tests">
  ## 无状态功能测试
</div>

运行针对以不同配置构建的 ClickHouse 二进制文件的[无状态功能测试](tests.md#functional-tests)——包括 release、debug、启用 sanitizer 等。
查看报告，确认哪些测试失败，然后按照[这里](/zh/development/tests#functional-tests)的说明在本地复现。
请注意，复现时必须使用正确的构建配置——某个测试可能在 AddressSanitizer 下失败，但在 Debug 下通过。
从 [CI 构建检查页面](/zh/install/advanced) 下载二进制文件，或在本地自行构建。

<div id="integration-tests">
  ## 集成测试
</div>

运行[集成测试](tests.md#integration-tests)。

<div id="bugfix-validate-check">
  ## Bugfix validate 检查
</div>

检查是否新增了测试 (functional 或 integration) ，或者是否有已修改的测试会在基于 master 分支构建的 binary 上失败。
当拉取请求带有 &quot;pr-bugfix&quot; 标签时，会触发此检查。

<div id="stress-test">
  ## 压力测试
</div>

从多个客户端并发运行无状态功能测试，以检测与并发相关的错误。如果失败：

* 先修复所有其他测试失败项；
  * 查看报告，找到服务器日志，并检查其中是否有可能导致错误的原因。

<div id="compatibility-check">
  ## 兼容性检查
</div>

检查 `clickhouse` 二进制文件是否能在使用旧版 libc 的发行版上运行。
如果检查失败，请向维护者寻求帮助。

<div id="ast-fuzzer">
  ## AST fuzzer
</div>

运行随机生成的查询，以发现程序错误。
如果失败，请向维护者寻求帮助。

<div id="performance-tests">
  ## 性能测试
</div>

用于衡量查询性能的变化。
这是耗时最长的检查项，运行时间接近 6 小时。
有关性能测试报告的详细说明，请参见[此处](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report)。