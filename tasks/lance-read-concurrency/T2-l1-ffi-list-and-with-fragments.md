# T2 — L1-FFI：list_fragments + with_fragments

| 字段 | 值 |
|------|-----|
| **ID** | T2 |
| **层** | L1 基础设施（FFI / Wrapper API only） |
| **Status** | done |
| **依赖** | 无（可与 T1 并行） |
| **被依赖** | **T3**（硬依赖） |
| **体量** | M |
| **目标 PR** | 独立 PR；合并后 T3 才能开工 |

---

## 1. 背景与问题

ClickHouse 侧要按 fragment 切任务，必须能：

1. 在 **pinned version** 下列出 fragment 元数据；  
2. 在 `plan_scan` 时把 scan **限制**到指定 fragment 子集。

Lance SDK（2.0.1）已提供：

- `Dataset::fragments()` / `get_fragments()`  
- `Fragment::id` (`u64`), `Fragment::num_rows() -> Option<usize>`  
- data file `file_size_bytes`（可用于估算 size）  
- `Scanner::with_fragments(Vec<Fragment>)`  
- `Scanner::from_fragment(...)`

当前 FFI **没有** list 接口；`ch_lance_plan_scan` 始终扫全表。

---

## 2. 目标

提供稳定、可取消、带错误分类的 C API + C++ wrapper：

1. `ch_lance_list_fragments`  
2. `ch_lance_plan_scan` 支持可选 `fragment_ids`  
3. C++：`DatasetHandle::listFragments` / `ScanDescription::fragment_ids`  
4. **不**改 `LanceDatasetIterator`、不改 `num_streams`（留给 T3）

**非目标**：pack 策略、多 stream、cluster、ProfileEvents 业务语义（T3）。

---

## 3. 设计

### 3.1 Fragment 信息结构

```c
typedef struct ch_lance_fragment_info
{
    uint64_t id;
    /// UINT64_MAX if unknown (Lance Option::None)
    uint64_t num_rows;
    /// 0 if unknown; best-effort sum of data file sizes
    uint64_t size_bytes;
} ch_lance_fragment_info;
```

### 3.2 List API

```c
/// Lists fragments for an exact dataset version (checkout_exact_version).
/// On success: *out_list allocated; caller frees with ch_lance_free_fragment_list.
/// cancel may be null.
bool ch_lance_list_fragments(
    ch_lance_dataset * dataset,
    uint64_t version,
    ch_lance_fragment_info ** out_list,
    size_t * out_size,
    ch_lance_cancel_handle * cancel,
    ch_lance_error * error);

void ch_lance_free_fragment_list(ch_lance_fragment_info * list, size_t size);
```

实现要点：

- `checkout_exact_version` 与 open/scan 一致；version 不存在 → `VERSION_NOT_FOUND`。  
- 使用现有 `FfiError` / kind / origin 映射。  
- 支持 cancel（`with_cancel`），与 `plan_scan` 同模式。  
- 空数据集：`out_size=0`，`out_list` 可为 null 或空分配；二者语义在文档写清（建议 size=0 且 list=null）。

### 3.3 Scan options 扩展

在 `ch_lance_scan_options` 追加（若 T1 已合并则接在 T1 字段之后）：

```c
/// null or size==0 → all fragments; else restrict with Scanner::with_fragments
const uint64_t * fragment_ids;
size_t fragment_ids_size;
```

Rust `plan_scan`：

```text
checkout version
build scanner (project/filter/limit/batch_size + T1 参数若已存在)
if fragment_ids_size > 0:
  resolve ids → Vec<Fragment> from dataset.fragments()
  missing id → INVALID_ARGUMENT or NOT_FOUND（推荐 BAD_ARGUMENTS / InvalidArgument）
  scanner.with_fragments(selected)
try_into_stream
```

**注意**：`with_fragments` 需要完整 `Fragment` 元数据，不能只传 id 而不从 manifest 解析；必须从 checkout 后的 dataset 按 id 查找。

### 3.4 C++ Wrapper

`LanceWrapper.h`：

```cpp
struct FragmentInfo
{
    UInt64 id = 0;
    /// nullopt if unknown
    std::optional<UInt64> num_rows;
    UInt64 size_bytes = 0;
};

// DatasetHandle:
std::vector<FragmentInfo> listFragments(
    const TableStateSnapshot & snapshot,
    const CancelHandlePtr & cancel = {}) const;

// ScanDescription:
std::vector<UInt64> fragment_ids; // empty = all
```

`planScan` 将 `fragment_ids` 指针传入 FFI（注意 vector 生命周期覆盖 `ch_lance_plan_scan` 调用）。

### 3.5 错误语义

| 情况 | kind → ClickHouse |
|------|-------------------|
| version 不存在 | VERSION_NOT_FOUND → 现有映射 |
| fragment id 不在该 version | INVALID_ARGUMENT |
| cancel | CANCELLED |
| 损坏 manifest | CORRUPT_DATA |

与 `LanceWrapper` 现有 `throwLanceError` / `ErrorMapping` 对齐。

---

## 4. 改动文件清单

| 文件 | 动作 |
|------|------|
| `rust/workspace/lance/include/ch_lance.h` | list API + scan options fragment_ids |
| `rust/workspace/lance/src/lib.rs` | 实现 + 单元测试（多 fragment 临时数据集） |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h` | `FragmentInfo`, `listFragments` |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.cpp` | 实现 |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h` | `fragment_ids` |
| `src/Storages/ObjectStorage/DataLakes/Lance/tests/gtest_*.cpp` | 可选 gtest 调 list/plan（若环境有数据） |
| **不要改** | `LanceMetadata::iterate` 业务逻辑（T3） |

---

## 5. 与 T1 的合并约定

若 T1 尚未合并：

- `fragment_ids` 与 T1 的 readahead 字段都追加在 `ch_lance_scan_options` **尾部**。  
- 本任务实现 `plan_scan` 时对「未使用的 T1 字段」若 struct 已含则传默认；若尚未含则只做 fragment。

若 T1 已合并：在其字段后追加 `fragment_ids`。

---

## 6. 测试与验证

### Rust 单测（必做）

1. 创建或使用多 fragment 临时 Lance dataset（lance write API 或拷贝 fixture）。  
2. `list_fragments` 返回 id 集合非空，与 SDK `fragments().len()` 一致。  
3. `plan_scan` 仅含 subset → `next_batch` 累计 rows ≤ 全表；两半 fragment 并集行数 = 全表（若无删除向量歧义）。  
4. 非法 fragment id → 错误 kind 正确。  
5. cancel list/plan → CANCELLED。  
6. 错误 version → VERSION_NOT_FOUND。

### C++

- 现有 Lance gtest 不回归。  
- 可选：gtest 打开 `data_lance` 路径 list（若 CI 有 USE_LANCE）。

### 验证命令

```bash
# 在 rust workspace 跑 lance crate 测试
cd rust/workspace && cargo test -p _ch_rust_lance 2>&1 | tee ../../build/test_lance_t2_rust.log
cd build && ninja unit_tests_dbms > build_lance_t2.log 2>&1
./unit_tests_dbms --gtest_filter='*Lance*' 2>&1 | tee test_lance_t2_gtest.log
```

---

## 7. 完成定义（DoD）

- [x] `list_fragments` / free API 完整且可取消  
- [x] `plan_scan` 支持 fragment 子集，缺 id 明确报错  
- [x] C++ `listFragments` + `ScanDescription::fragment_ids` 可用  
- [x] Rust 单测覆盖 list/subset/非法 id/cancel  
- [x] **未**改变生产查询路径的 stream 数（仍 1 object）  
- [x] Status → `done`；README 更新  

---

## 8. 风险

| 风险 | 缓解 |
|------|------|
| `num_rows` / size 未知 | 用 `UINT64_MAX` / 0；T3 pack 算法要处理 unknown |
| Fragment id 类型 u64 vs CH UInt64 | 统一 UInt64 |
| 大表 list 分配 | 一次分配数组；T3 可缓存 list 到 QuerySession |
| 与删除向量 | list 仍返回 fragment；行数可能为 max 含删；正确性依赖 SDK scan |

---

## 9. 回滚

本任务仅 API 扩展，无用户行为变化；回滚即 revert commit。生产路径未调用 list 前无用户可见影响。
