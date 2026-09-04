# T3 — L1-CH：Fragment pack 多 stream

| 字段 | 值 |
|------|-----|
| **ID** | T3 |
| **层** | L1 ClickHouse pipeline（主收益） |
| **Status** | done |
| **依赖** | **T2 必须完成**（list_fragments + with_fragments） |
| **被依赖** | T4、T5 |
| **体量** | L（主交付） |
| **目标 PR** | 独立中大型 PR；可拆 commit：iterator → 守卫 → 测试 |

---

## 1. 背景与问题

`ReadFromObjectStorageStep::initializePipeline`：

```text
if estimatedKeysCount > 1:
  num_streams = min(num_streams, estimatedKeysCount)
else:
  num_streams = 1
```

当前 `LanceDatasetIterator`：

- `next()` 只返回 **一次** synthetic `LanceDatasetObjectInfo`  
- `estimatedKeysCount()` 返回 0 或 1  

因此无论 `max_threads` 多大，Lance 读永远单 stream。  
Iceberg 按 data file 产出多个 `ObjectInfo`，天然多 stream。

T2 已提供 list/with_fragments；T3 把 **fragment pack** 变成多个 `ObjectInfo`，打通 CH 并行。

---

## 2. 目标

1. `iterate` 产出 **N 个** fragment pack 任务（N≥1）。  
2. `estimatedKeysCount() == N`，使 `num_streams > 1` 成为可能。  
3. 每个 `LanceReadSource` 只 `planScan` 自己的 `fragment_ids`。  
4. 共享 query-scoped `DatasetHandle` + pinned version。  
5. **语义守卫**：LIMIT / 有序 / count 快路径不被破坏。  
6. Settings 可一键关闭并行，回退单 pack。

**非目标**：cluster 表函数（T4）；系统压测报告（T5）；改 Iceberg 代码。

---

## 3. 设计

### 3.1 任务单元：Fragment Pack

```text
FragmentPack {
  version: UInt64
  fragment_ids: vector<UInt64>
  estimated_rows: optional
  estimated_bytes: UInt64
}
```

### 3.2 ObjectInfo

扩展或替换现有 `LanceDatasetObjectInfo`（`LanceMetadata.cpp` 内）：

```text
LanceFragmentObjectInfo : ObjectInfo  (或扩 LanceDatasetObjectInfo)
  - relative path: 稳定非空假路径，例如
      "{dataset_uri}#v{version}/pack{index}_f{firstId}"
  - snapshot: TableStateSnapshot
  - dataset: DatasetHandle (可选，与 session 一致)
  - fragment_ids: vector<UInt64>   // 空仅用于 force single full-scan 兼容路径
  - pack_index / pack_count（可选，调试）
  - ObjectMetadata: is_size_known=false（避免错误 S3 Head）
```

`extractLanceObjectInfo` 必须识别新类型。

### 3.3 Iterator

`LanceDatasetIterator` 改造：

**构造 / 首次 next 前**：

```text
1. dataset = session.getPinned(...)
2. fragments = dataset.listFragments(snapshot, cancel?)  // T2 API
3. if !enable_fragment_parallelism or force_single_stream_reason:
     packs = [ Pack{all fragment ids or empty meaning full} ]
   else:
     packs = partitionIntoPacks(fragments, settings, max_threads_hint)
4. estimatedKeysCount = packs.size()
5. next() 原子弹出 packs
```

**`estimatedKeysCount()`**：返回 pack 总数（构造时算好），不能在 next 后变成 0 导致后续 stream 误判——对照 Iceberg：通常返回预估值；当前实现 `is_finished ? 0 : 1` 对多 stream **危险**。  
**要求**：`estimatedKeysCount()` 在整个生命周期返回 **初始 pack 数**（与 Iceberg 类似用稳定估计），或返回 `max(remaining, 1)` 但 **initializePipeline 调用时必须 >1**。  
推荐：成员 `const size_t total_packs`，`estimatedKeysCount() const { return total_packs; }`。

### 3.4 Pack 切分算法

Settings：

| Setting | 类型 | 默认 | 含义 |
|---------|------|------|------|
| `lance_enable_fragment_parallelism` | Bool | `true` | L1 总开关 |
| `lance_fragment_pack_mode` | 见下 | `auto` | 切分模式 |
| `lance_max_fragment_packs` | UInt64 | `0` | 0 → 对齐 `max_threads`；否则 pack 数上限 |
| `lance_min_rows_per_pack` | UInt64 | `0` | 合并门槛（0=忽略） |
| `lance_min_bytes_per_pack` | UInt64 | `0` | 合并门槛（0=忽略） |

**Mode**（可用 String 或自定义 enum；若枚举成本高，用 String：`one` / `pack` / `auto`）：

| Mode | 行为 |
|------|------|
| `one` | 1 fragment = 1 pack |
| `pack` | 填桶到 `target_packs` |
| `auto` | `fragments.size() ≤ target_packs` → one；否则 pack |

**target_packs**：

```text
target = lance_max_fragment_packs == 0
  ? max(1, max_threads_from_context)
  : max(1, lance_max_fragment_packs)
```

`max_threads` 来源：`Context` 的 `max_threads` 设置；`iterate` 若拿不到可退化为 `std::thread::hardware_concurrency()` 封顶 32，或仅用 `lance_max_fragment_packs`。

**填桶（LPT / 轮询）**：

- 权重：`num_rows` 若 known，否则 `size_bytes` 若 >0，否则 `1`。  
- 按权重降序，每次放进当前总权重最小的桶。  
- 空 id 列表的 pack 不要产生。

### 3.5 ScanDescription / read()

```cpp
// ScanDescription 已有 fragment_ids (T2)
// read():
scan.fragment_ids = lance_object.fragment_ids;
// 若 enable 且 force full single: fragment_ids empty = all (T2 语义)
```

每个 pack 一个 `LanceReadSource`；predicate/limit/projection 逻辑保持，但受 §3.6 守卫。

### 3.6 语义守卫（必须实现）

| 条件 | 行为 |
|------|------|
| `need_only_count && predicate_is_complete` | **单 pack / 不切 fragment**；走现有 `countRows`/`totalRows` 快路径 |
| 可安全下推的 `LIMIT`（现有：`limit && predicate_is_complete`） | **强制单 stream**（一个 full pack 或 empty fragment_ids 全表 scan + limit 下推） |
| 需要确定性输出顺序（若上层 `requestReadingInOrder` 或未来 ORDER 保证） | **单 stream + `lance_scan_in_order=true`** |
| `!lance_enable_fragment_parallelism` | 单 pack 全表 |
| partial predicate | **允许多 pack**；残差 CH filter |
| 0 fragments | iterator 立即结束；空结果 |

实现建议：在 `iterate` 入口根据 `filter_dag`/context **无法** 完整知道 limit/order 时：

- Limit 信息在 `read()` / `ScanDescription` 更完整。  
- **策略 A（推荐）**：`iterate` 默认多 pack；`LanceMetadata::read` 若发现 `scan.limit` 将设置，则 **忽略 fragment 子集、扫全表** 仅当单 stream——但多 Object 已创建会导致多 source 各带 limit → **错误**。  

**正确策略**：

```text
Limit / ordered 必须在 iterate 阶段就 force_single_pack。

问题：iterate 签名目前无 limit。
```

`IDataLakeMetadata::iterate` 当前：

```cpp
ObjectIterator iterate(filter_dag, callback, list_batch_size, storage_metadata, context)
```

**可行方案（选一，实现时写清）**：

1. **Context 查询设置 + format filter 尚不完整时偏保守**：仅当 settings 允许且无「明显需要单流」时多 pack；LIMIT 由 `SourceStepWithFilter::limit` 在更早路径传入——查 `ReadFromObjectStorageStep` 的 `limit` 成员是否在 `createFileIterator` 前可知。  
2. **iterate 始终多 pack；read() 发现 limit 时抛 LOGICAL_ERROR 或降级**：降级困难（已多 object）。  
3. **推荐落地**：扩展调用链，使 `createFileIterator` / `iterate` 能读到 `ReadFromObjectStorageStep::limit`（或 SelectQueryInfo 中的 limit）。若改动面大，**第一版：有 limit 下推意图时在 iterate 用 settings `lance_enable_fragment_parallelism` 文档要求用户关；代码内：若 `FormatFilterInfo` 不可用，则对 `limit` 在 `StorageObjectStorageSource` 传入的 `limit_` 在 **第一个** source 处理——不可靠。  

**务实第一版（写进实现）**：

```text
force_single_pack IF any of:
  - !lance_enable_fragment_parallelism
  - need_only_count path will be used (iterate 不知道 need_only_count...)
```

`need_only_count` 在 `StorageObjectStorage::read` → `ReadFromObjectStorageStep` 有，但 **iterate 时** 在 `createFileIterator`，此时 `need_only_count` 已算好并传入 Source，**未传入 iterate**。

查看 `createFileIterator` → `configuration->iterate`：T3 实现时 **修改** `IDataLakeMetadata::iterate` 或 Lance 专用路径，把「并行安全」标志从 `ReadFromObjectStorageStep::createIterator` 传入。

最小侵入选项：

- 在 `Context` kitchen_sink / query 级设置 `lance_force_single_fragment_pack` 由 `ReadFromObjectStorageStep` 在 createIterator 前根据 `need_only_count || limit` 写入；  
- 或给 `StorageObjectStorageConfiguration::iterate` / data lake iterate 增加可选参数（改动 Iceberg 默认参数需 default）。

**任务要求**：实现者必须保证 **LIMIT 多 stream 不会错误截断结果**；优先改 `ReadFromObjectStorageStep::createIterator` 向 Lance 传递：

```text
struct LanceIterateOptions {
  bool force_single_pack = false; // need_only_count || (limit.has_value() && limit>0) || ordered
};
```

若 limit 存在但 predicate 不完整（不能下推 limit），**仍可多 pack**（各 stream 全读 + CH limit）——过读但正确。仅 **`predicate_is_complete && limit`** 时需单 pack 才能保持 limit 下推。

iterate 阶段 predicate 完整性：可复用 `extractLancePredicatePushdown` 逻辑（从 filter_dag），与 `read` 一致。

### 3.7 Dataset / Session

- 所有 pack 共用 `QuerySession::getPinned`。  
- 可选：`QuerySession` 缓存 `listFragments` 结果（key: identity + version），避免 iterate 与别处重复 list。  
- **禁止** 每 pack `openEphemeral`。

### 3.8 ProfileEvents

| Event | 含义 |
|-------|------|
| `LanceFragmentsListed` | list 返回的 fragment 数 |
| `LanceFragmentPacks` | pack 数 |
| `LanceFragmentParallelismDisabled` | 因守卫/开关走单 pack 的次数 |
| `LanceStreams` | 可选：由外部推断 |

已有 open/plan/next/pushdown counters 保持。

### 3.9 与 L2（T1）叠加

多 pack 时默认建议：

- 无序需求：`lance_scan_in_order=0` 可由文档推荐；代码默认仍 true 除非 settings。  
- 每 pack fragment 少时 readahead 收益小，无妨同时打开。

---

## 4. 改动文件清单

| 文件 | 动作 |
|------|------|
| `LanceMetadata.cpp` | ObjectInfo、Iterator、pack、read 接线、守卫 |
| `LanceMetadata.h` | 若 iterate 签名需扩展则改基类默认 |
| `IDataLakeMetadata.h` / `DataLakeConfiguration` | 仅当必须扩展 iterate 参数 |
| `ReadFromObjectStorageStep.cpp` | 向 iterator 传递 force_single_pack / limit 意图 |
| `LanceScanDescription.h` | 已有 fragment_ids（T2） |
| `LanceReadSource.cpp` | 确认 cancel/progress 多实例安全 |
| `LanceQuerySession.*` | 可选 fragment list 缓存 |
| `Core/Settings.cpp` | L1 settings |
| `Common/ProfileEvents.cpp` | 新 events |
| `tests/queries/0_stateless/` | 多 fragment 数据集 + SQL |
| `tests/integration/test_s3_table_functions/test.py` | 多 pack 正确性 + counter（可选） |
| `docs/.../lance.md` | 限制与 settings |

---

## 5. 测试数据

现有 `data_lance` 可能 fragment 很少。需要：

1. **生成脚本**（Python/lance 或文档化手动步骤）创建 ≥8 fragments 的 dataset，固定行与 checksum。  
2. 放入 `tests/queries/0_stateless/data_lance/multi_frag.lance/`（或 integration 动态生成）。  
3. SQL：  
   - `SELECT count(), sum(id) FROM lanceLocal(...)` 与 `SET lance_enable_fragment_parallelism=0` 对比。  
   - ProfileEvents：`LanceFragmentPacks > 1`（`04627` 风格）。  
   - `LIMIT` 正确性。  
   - filter pushdown 下结果一致。

---

## 6. 测试与验证

### 必做

1. 单 fragment：行为与并行关闭时一致，packs=1。  
2. 多 fragment：并行开/关校验和一致。  
3. LIMIT：结果行数正确。  
4. count 快路径仍走（ProfileEvents `LanceCountRows`）。  
5. 取消查询不挂死。  
6. 现有全部 `*lance*` stateless + S3 integration 不回归。

### 验证命令

```bash
cd build && ninja clickhouse > build_lance_t3.log 2>&1
./tests/clickhouse-test $(ls tests/queries/0_stateless/*lance*.sql | xargs -n1 basename | sed 's/\.sql$//')
# integration
# python -m ci.praktika run "integration" --test test_s3_table_functions
```

---

## 7. 完成定义（DoD）

- [x] 多 fragment 时 `estimatedKeysCount>1` 且实测多 stream（query log / events）  
- [x] 结果与 `lance_enable_fragment_parallelism=0` 一致（无序时 sort 后比）  
- [x] LIMIT / count / 开关守卫正确  
- [x] Dataset 查询内复用，无 per-pack open 风暴  
- [x] 测试 + 文档  
- [x] Status → `done`；README 更新  

---

## 8. 风险

| 风险 | 缓解 |
|------|------|
| LIMIT 错误 | force_single_pack；测试 |
| estimatedKeysCount 生命周期 | 固定 total_packs |
| S3 Head 假路径 | size unknown；自定义 read 不走 buffer |
| 过多 pack 调度开销 | max packs / auto pack |
| 与 T1 无序组合 flaky | checksum 排序后比较 |

---

## 9. 回滚

```sql
SET lance_enable_fragment_parallelism = 0;
```

立即回到单 pack 全表 scan（可仍带 T1 readahead 与 T2 API）。
