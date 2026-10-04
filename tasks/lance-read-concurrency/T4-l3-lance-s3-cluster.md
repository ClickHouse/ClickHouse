# T4 — L3：lanceS3Cluster 跨节点分片

| 字段 | 值 |
|------|-----|
| **ID** | T4 |
| **层** | L3 集群分布式读 |
| **Status** | done |
| **依赖** | **T3 必须完成**（fragment pack 任务模型） |
| **被依赖** | T5（可选集群压测） |
| **体量** | L |
| **目标 PR** | 独立 PR（勿与 T3 混） |

---

## 1. 背景与问题

单机 L1 多 stream 受单节点 CPU/网卡限制。Iceberg/Delta/Paimon 已有 `*Cluster` 表函数：initiator 列任务，replicas 通过 `ClusterFunctionReadTask` 拉取 `ObjectInfo` 分片读。

Lance 仅有 `lanceS3` / `LanceS3`，无 cluster 入口；且在 T3 之前任务粒度是整表，无法分片。

T3 之后任务粒度 = **fragment pack**，可序列化分发。

---

## 2. 目标

1. 注册 `lanceS3Cluster` 表函数（及 Definition）。  
2. Initiator：pin version → list fragments → pack → 进入 `StorageObjectStorageStableTaskDistributor`。  
3. Worker：反序列化 pack（version + fragment_ids）→ 精确 version scan。  
4. 多节点结果与单机 `lanceS3` 一致。  
5. 文档 + Experimental badge。

**可选（本任务可不做，除非成本低）**：`lanceLocalCluster`。

**非目标**：Azure/GCS cluster；改写通用 cluster 框架；向量检索。

---

## 3. 设计

### 3.1 注册（对齐 Iceberg）

参考：

- `src/Storages/ObjectStorage/StorageObjectStorageDefinitions.h` — `IcebergS3ClusterDefinition`  
- `src/TableFunctions/TableFunctionObjectStorageCluster.h` — `TableFunctionIcebergS3Cluster`  
- `src/TableFunctions/TableFunctionObjectStorageCluster.cpp` — `registerTableFunctionIcebergCluster`

新增：

```cpp
struct LanceS3ClusterDefinition
{
    static constexpr auto name = "lanceS3Cluster";
    // storage_engine_name / 其它字段对齐同文件内 Cluster 定义模式
};
```

```cpp
using TableFunctionLanceS3Cluster =
    TableFunctionObjectStorageCluster<LanceS3ClusterDefinition, StorageS3LanceConfiguration, true>;
```

`registerTableFunctionObjectStorageCluster` 中 `registerFunction<TableFunctionLanceS3Cluster>`，`#if USE_LANCE` 与 S3 条件与 `lanceS3` 一致。

配置类型复用 **`StorageS3LanceConfiguration`**（与 `lanceS3` 相同 data lake metadata = `LanceMetadata`）。

### 3.2 任务内容

Initiator 侧 iterator（T3 的 pack iterator）在 **distributed_processing** 路径下：

- Coordinator：本地 `iterate` 产生 packs，经 `StableTaskDistributor` 分发。  
- Worker：`ReadTaskIterator` 回调拿到序列化 path/object，反序列化为 `LanceFragmentObjectInfo`。

### 3.3 序列化

对齐 `IcebergObjectSerializableInfo`：

```text
LanceObjectSerializableInfo {
  version: UInt64
  fragment_ids: vector<UInt64>
  // 可选 pack_index
}
```

实现位置建议：

- `LanceFragmentObjectInfo` 或独立 `LanceObjectSerializableInfo`  
- `serializeForClusterFunctionProtocol` / `deserializeForClusterFunctionProtocol`  
- 与 `ClusterFunctionReadTaskResponse` / ObjectInfo 路径对接（读 Iceberg 如何挂上 `data_lake_metadata` 或 path 编码）

**禁止** 把 access_key/secret 写入 task；凭据走 named collection / 节点配置 / 与 s3Cluster 相同通道。

**URI**：worker 必须能解析同一 bucket/key；与 `StorageObjectStorageCluster` 现有路径拼接方式一致。

### 3.4 Version 一致性

| 规则 | 说明 |
|------|------|
| Initiator pin | 分析期 / iterate 前 pin version，写入每个 task |
| Worker | `checkout_exact_version`；**禁止** 读 latest |
| Pin 丢失 | 查询失败（VERSION_NOT_FOUND），不静默 |

跨节点 **不要** 依赖 `QuerySession` 进程内缓存互通；worker 各自 open + pin 到 task 内 version。可复用 worker 进程内 QuerySession 仅作同查询复用。

### 3.5 切分粒度（集群）

| 角色 | 建议 |
|------|------|
| Initiator packs | `≈ nodes × max_streams_for_files_processing_in_cluster_functions` 或 `lance_max_fragment_packs` 显式调大 |
| Worker 内 | 通常 1 task = 1 pack = 1 本地 stream；L2 readahead 仍可开 |

Settings：可复用 T3 的 pack settings；可选新增 `lance_cluster_target_packs_per_replica`（非必须，T5 再调）。

### 3.6 语义守卫

与 T3 相同逻辑在 initiator 生效：

- count 快路径 / LIMIT 单 pack：可能 **无法** 集群加速 → 单节点执行或单 task；需与现有 iceberg cluster 行为对齐。  
- 第一版可接受：LIMIT 查询不走有效分片（单 task），文档标明。

### 3.7 失败与取消

- Worker 失败向上传播。  
- 查询取消：现有 cluster 取消路径 + 本地 `LanceReadSource::onCancel`。

---

## 4. 改动文件清单

| 文件 | 动作 |
|------|------|
| `StorageObjectStorageDefinitions.h` | `LanceS3ClusterDefinition` |
| `TableFunctionObjectStorageCluster.h` | using alias |
| `TableFunctionObjectStorageCluster.cpp` | register + 文档字符串 |
| `TableFunctionObjectStorage.cpp` | 若 cluster 名称列表有特殊分支，加入 `lanceS3Cluster` |
| `LanceMetadata.cpp` / 新文件 | 序列化、distributed 路径 ObjectInfo |
| `StorageObjectStorageCluster.cpp` | 仅当 Lance 需特殊分支（尽量不改） |
| `Analyzer` / cluster alternatives visitor | 若需 `lanceS3` → `lanceS3Cluster` 自动改写（对照 iceberg） |
| `docs/en/sql-reference/table-functions/lance.md` | cluster 语法 |
| `docs/en/engines/.../lance.md` | 互链 |
| `tests/integration/` | 多节点测试（新建 `test_lance_s3_cluster` 或并入现有 cluster 测试夹具） |

查阅 `TableFunctionsWithClusterAlternativesVisitor` 是否需登记 `lanceS3`↔`lanceS3Cluster`。

---

## 5. 测试与验证

### 必做 Integration

1. 2+ ClickHouse 节点 + MinIO。  
2. 上传 **多 fragment** Lance 数据集。  
3. `SELECT count(), sum(...) FROM lanceS3Cluster(cluster, ...)`  
   与 initiator 本地 `lanceS3` 对比。  
4. 验证 tasks 被多节点处理（system 日志 / 各节点 ProfileEvents / trace）。  
5. 错误：错误凭据、missing dataset、pinned version 删除。  
6. `USE_LANCE` 关闭构建下函数不可用（skip 或未注册）。

### 参考

- `tests/integration/test_s3_table_functions/test.py`（Lance S3 单机）  
- Iceberg cluster integration（`test_storage_iceberg*` / cluster 相关）分发模式  

### 验证命令

```bash
# 按仓库 integration 惯例
python -m ci.praktika run "integration" --test lance
# 或具体模块名实现后填写
```

---

## 6. 完成定义（DoD）

- [x] `lanceS3Cluster` 已注册且 Experimental 文档齐全  
- [x] 多节点结果 = 单机 `lanceS3`  
- [x] task 含 version + fragment_ids；无密钥泄漏  
- [x] worker 不读 unpinned latest  
- [x] integration 测试稳定  
- [x] Status → `done`；README 更新  

---

## 7. 风险

| 风险 | 缓解 |
|------|------|
| 协议版本不兼容 | 跟随现有 cluster protocol 扩展方式；测试 serde |
| Initiator/worker schema 不一致 | 共用 pin version + 同一 configuration |
| 小表集群负加速 | 文档：小表用 lanceS3 |
| T3 pack 在 worker 二次切分混乱 | Worker 不二次 list；只扫 task 内 ids |

---

## 8. 回滚

- 用户停止使用 `lanceS3Cluster`，改回 `lanceS3`。  
- 代码：`#if USE_LANCE` 注册可整段禁用。  
- 不依赖 T4 的 T3 单机路径不受影响。

---

## 9. 实现检查清单（执行时勾选）

- [x] Definition + TableFunction 注册  
- [x] ObjectInfo 集群序列化 round-trip gtest  
- [x] distributed_processing 路径走 pack iterator  
- [x] Worker extractLanceObjectInfo + fragment_ids scan  
- [x] 文档示例 SQL  
- [x] Integration 多节点  
- [x] Cluster alternatives visitor（若适用） — 自动 `name+Cluster` 路径，无需 visitor 映射  
