# Lance 只读并发最大化

将 ClickHouse 读取 Lance 的并发从「整表单 stream」提升到三层叠加模型：

1. **L2** — Lance Scanner 内部 IO 并行（fragment/batch readahead）
2. **L1** — ClickHouse 侧 fragment pack 多 stream
3. **L3** — `lanceS3Cluster` 跨节点分片

有效并发目标：

```text
min(max_threads, fragment packs, SDK fragment IO, nodes × streams, 存储带宽)
```

## 子任务索引

| ID | 文件 | 层 | 标题 | 依赖 | 体量 |
|----|------|----|------|------|------|
| T1 | [T1-l2-scanner-internal-parallelism.md](T1-l2-scanner-internal-parallelism.md) | L2 | Scanner 内部并行 | 无 | S |
| T2 | [T2-l1-ffi-list-and-with-fragments.md](T2-l1-ffi-list-and-with-fragments.md) | L1-FFI | list_fragments + with_fragments | 无（可与 T1 并行） | M |
| T3 | [T3-l1-ch-multi-stream.md](T3-l1-ch-multi-stream.md) | L1-CH | Fragment pack 多 stream | **T2** | L |
| T4 | [T4-l3-lance-s3-cluster.md](T4-l3-lance-s3-cluster.md) | L3 | lanceS3Cluster | **T3** | L |
| T5 | [T5-benchmarks-and-docs.md](T5-benchmarks-and-docs.md) | 验收 | 压测与文档 | T1+T3（T4 可选） | M |

## 依赖图

```text
T1 ──────────────┐
                 ├──→ T5
T2 ──→ T3 ──→ T4 ┘
```

## 执行顺序建议

1. **并行**：T1 ∥ T2  
2. **主路径**：T3（单机吞吐主收益）  
3. **横向**：T4  
4. **收尾**：T5  

## 全局原则（所有子任务遵守）

- 只读；不引入写入 / 向量索引 / compaction。
- 查询级 **version pin** 贯穿所有 stream / 节点；pin 丢失则报错，不读 latest。
- 查询内 **Dataset handle 复用**（`Lance::QuerySession`）；不因切 fragment 重复 open。
- **语义守卫优先于吞吐**：`LIMIT` / 有序读 / `count` 快路径不得被错误并行破坏。
- 每层提供 settings **回滚开关**；默认行为可渐进打开。
- C++ 使用 Allman 大括号；commit 不加 AI 痕迹；不主动 push；不 rebase/amend（新 commit）。
- 构建：`cd build && ninja ...` 日志重定向到 build 目录；测试日志同理。

## 现状锚点（实现时对照）

| 组件 | 路径 |
|------|------|
| FFI | `rust/workspace/lance/include/ch_lance.h`, `rust/workspace/lance/src/lib.rs` |
| Wrapper | `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.{h,cpp}` |
| 元数据/读 | `LanceMetadata.cpp`, `LanceReadSource.cpp`, `LanceScanDescription.h` |
| Session | `LanceQuerySession.{h,cpp}` |
| 多 stream 入口 | `src/Processors/QueryPlan/ReadFromObjectStorageStep.cpp`（`estimatedKeysCount`） |
| Iterator 现状 | `LanceDatasetIterator` 只 yield **1** 个 synthetic object |
| Cluster 范例 | `icebergS3Cluster`：`TableFunctionObjectStorageCluster.cpp`, `IcebergDataObjectInfo` 序列化 |
| Lance SDK | crate `lance` 2.0.1：`Scanner::with_fragments`, `fragment_readahead`, `scan_in_order`, `Dataset::get_fragments` / `fragments()` |
| 本地测试数据 | `tests/queries/0_stateless/data_lance/` |
| S3 integration | `tests/integration/test_s3_table_functions/test.py` |
| 文档 | `docs/en/engines/table-engines/integrations/lance.md`, `docs/en/sql-reference/table-functions/lance.md` |

## 完成状态

| ID | 状态 |
|----|------|
| T1 | done |
| T2 | done |
| T3 | done |
| T4 | done |
| T5 | pending |

更新子任务完成时同步改本表与对应文件顶部的 `Status`。
