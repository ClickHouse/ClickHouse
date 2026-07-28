# T5 — 压测、调参与文档收尾

| 字段 | 值 |
|------|-----|
| **ID** | T5 |
| **层** | 验收 / 产品化包装 |
| **Status** | pending |
| **依赖** | **T1 + T3** 必须；**T4 可选**（有则补集群一节） |
| **被依赖** | 无 |
| **体量** | M |
| **目标 PR** | 可独立 PR（文档+脚本+结果摘要）；或并入 T3 尾部 |

---

## 1. 背景与问题

T1–T4 交付功能后，若无线上可参考的：

- 何时开并行  
- 推荐 settings  
- 相对基线的吞吐数字  
- 已知限制（LIMIT/有序/小表）  

则生产试点仍无法「按手册」放量。本任务补齐 **可重复压测** 与 **用户可见文档**，不新增核心读路径功能。

---

## 2. 目标

1. 可重复的本地/S3 压测方法（脚本或文档化步骤）。  
2. 基线对比矩阵：关闭并行 / 仅 L2 / L1+L2 /（可选）L3。  
3. 更新官方文档：settings、并发模型、限制、推荐配置。  
4. 将结论回写本目录 `RESULTS.md`（新建）供后续迭代。

**非目标**：改算法；GA 摘 Experimental；对象存储 FS cache 专项。

---

## 3. 压测设计

### 3.1 数据集

| 集 | 特征 | 用途 |
|----|------|------|
| A | 1–2 fragments，窄表 | 确认无回归、无负优化 |
| B | ≥32 fragments，中等行数，窄表 | L1 stream 扩展 |
| C | ≥32 fragments，宽表（多列 String） | Arrow→CH 转换并行收益 |
| D | 与 B 相同，S3/MinIO | 网络 IO + L2 readahead |
| E | （可选）B 的 10× 行 | 饱和曲线 |

数据可：

- 用 Lance/Python 生成后提交小型 B 到 `tests/...`（注意仓体积）；或  
- 脚本生成到 `tmp/lance_bench/`（**不**强制进 git）。

记录：fragment 数、总行数、列 schema、生成命令。

### 3.2 查询模板

```sql
-- Q1 全表 count（应走快路径，验证未被错误并行破坏）
SELECT count() FROM lance...

-- Q2 全表聚合（主吞吐）
SELECT count(), sum(id), sum(length(s)) FROM lance...

-- Q3 高选择率 filter
SELECT count() FROM lance... WHERE id % 2 = 0

-- Q4 低选择率
SELECT count() FROM lance... WHERE id = 1

-- Q5 LIMIT
SELECT * FROM lance... LIMIT 100

-- Q6 投影窄列
SELECT id FROM lance...
```

### 3.3 配置矩阵

| 配置名 | Settings |
|--------|----------|
| **base** | `lance_enable_fragment_parallelism=0`, readahead 全 0, `lance_scan_in_order=1` |
| **L2** | parallelism=0, `lance_scan_in_order=0`, `lance_fragment_readahead=8`, batch 默认或 8 |
| **L1** | parallelism=1, pack auto, scan_in_order=1, readahead 0 |
| **L1+L2** | parallelism=1, scan_in_order=0, fragment_readahead=4 |
| **L3** | `lanceS3Cluster` + L1+L2（若 T4 完成） |

固定：`max_threads`、同机器、清 OS page cache（S3 冷读时可选重启/换 key）。

### 3.4 指标

| 指标 | 来源 |
|------|------|
| wall time | `clickhouse-client` 或 `system.query_log` |
| rows/s | rows / time |
| `LanceFragmentPacks` / `LanceStreams` | query_log ProfileEvents |
| `LanceNextBatch*` / `LanceArrowConvert*` | query_log |
| 错误 | 无 |

每配置每查询至少 **5 次**，取中位数。

### 3.5 成功参考（非硬门禁，写入 RESULTS）

| 场景 | 期望 |
|------|------|
| 集 B/C + Q2，L1+L2 vs base | 吞吐明显上升（视核数，目标方向 ≥2×，理想 3–8×） |
| 集 A | 无显著回退（±10% 噪声内） |
| Q1 count | 时间接近 base（快路径） |
| Q5 LIMIT | 正确且不显著变慢于 base |
| L3 vs 单机 | 随节点增加有正向扩展直至瓶颈 |

若未达预期：在 `RESULTS.md` 记录瓶颈假设（fragment 太少、S3、转换、Tokio threads）。

---

## 4. 交付物

### 4.1 脚本（推荐路径）

```text
tasks/lance-read-concurrency/bench/
  README.md           # 如何跑
  generate_dataset.py # 或 .sh
  run_bench.sh        # 调 clickhouse-client，吐 CSV
  compare.py          # 可选：汇总中位数
```

脚本约定：

- 不依赖外网（S3 用本地 MinIO 或文件 URI）。  
- 结果默认写 `tasks/lance-read-concurrency/bench/out/`（gitignore 可加 `out/`）。

### 4.2 RESULTS.md

路径：`tasks/lance-read-concurrency/RESULTS.md`

至少包含：

- 日期、commit SHA、机器规格（CPU/RAM/磁盘）  
- 数据集描述  
- 结果表（配置 × 查询 × 中位时延）  
- 推荐默认 settings  
- 已知问题列表  

### 4.3 官方文档更新

**文件**：

- `docs/en/engines/table-engines/integrations/lance.md`  
- `docs/en/sql-reference/table-functions/lance.md`  
- 若 T4 完成：cluster 专节  

**内容要点**（英文文档，header 带 `{#anchor}`）：

1. **Read parallelism** 专节：三层模型一句话 + 图/列表。  
2. Settings 表：`lance_enable_fragment_parallelism`, pack 相关, T1 readahead, `lance_runtime_threads`, `lance_query_dataset_reuse`。  
3. **When parallelism is disabled**：LIMIT 下推、ordered、count、开关。  
4. **Recommendations**：大表多 fragment → 开 L1+L2；小表默认即可；集群用 `lanceS3Cluster`。  
5. 仍保留 Experimental badge。  
6. ProfileEvents 列表与 T3 对齐。

---

## 5. 改动文件清单

| 路径 | 动作 |
|------|------|
| `tasks/lance-read-concurrency/bench/*` | 新建脚本与说明 |
| `tasks/lance-read-concurrency/RESULTS.md` | 压测记录 |
| `docs/en/engines/table-engines/integrations/lance.md` | 并行与 settings |
| `docs/en/sql-reference/table-functions/lance.md` | 同上 + cluster |
| `.gitignore` | 可选忽略 `tasks/**/bench/out/` |

---

## 6. 完成定义（DoD）

- [ ] bench 脚本可按 README 复跑  
- [ ] `RESULTS.md` 含至少 base vs L1+L2 数字  
- [ ] 官方文档描述并行模型、settings、限制、推荐  
- [ ] 与 T1/T3（T4）实现一致，无过时描述  
- [ ] Status → `done`；README 总表更新  

---

## 7. 风险

| 风险 | 缓解 |
|------|------|
| 数据过大进 git | 脚本生成；只提交小 fixture |
| 环境噪声 | 多次中位数；写明机器 |
| 文档与代码漂移 | 以 Settings.cpp 名为准 |

---

## 8. 回滚

文档/脚本可独立 revert；不影响运行时。

---

## 9. 执行检查清单

- [ ] 确认 T1/T3 已合并到工作分支  
- [ ] 生成数据集 B/C  
- [ ] 跑配置矩阵  
- [ ] 写 RESULTS.md  
- [ ] 更新 docs  
- [ ] （可选）T4 集群两点对比  
- [ ] 更新本任务与 README Status  
