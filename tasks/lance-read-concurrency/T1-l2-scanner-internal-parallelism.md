# T1 — L2：Lance Scanner 内部并行

| 字段 | 值 |
|------|-----|
| **ID** | T1 |
| **层** | L2（SDK 内部 IO 并行） |
| **Status** | done |
| **依赖** | 无（可与 T2 并行） |
| **被依赖** | T5（压测对比） |
| **体量** | S（约 0.5–1 周） |
| **目标 PR** | 独立小 PR，可先于 T3 合并 |

---

## 1. 背景与问题

当前 `ch_lance_plan_scan` 只设置了 `project` / `filter` / `batch_size` / `limit`，**未暴露** Lance Scanner 的：

- `scan_in_order`
- `fragment_readahead`（需 `scan_in_order=false` 才生效）
- `batch_readahead`
- `io_buffer_size`

即使仍是单 `LanceReadSource`，也未打开 SDK 内部的 fragment/batch 预取，S3 冷读带宽利用率偏低。

参考 SDK（lance 2.0.1）：

- `Scanner::scan_in_order(bool)`
- `Scanner::fragment_readahead(usize)`
- `Scanner::batch_readahead(usize)`
- `Scanner::io_buffer_size(u64)`
- 默认 `batch_readahead` 约等于 compute-intensive CPU 数；`fragment_readahead` 默认 `None`

---

## 2. 目标

在**不改变 CH pipeline 任务切分**的前提下：

1. 将 Scanner 内部并行参数经 FFI → C++ → Settings 贯通。
2. 默认保持**有序、行为兼容**（`scan_in_order=true`）。
3. 关闭/归零参数时行为与现状一致（功能回归绿）。

**非目标**：多 `ObjectInfo`、多 stream、cluster（属 T2/T3/T4）。

---

## 3. 设计

### 3.1 FFI（`ch_lance_scan_options` 扩展）

在 `rust/workspace/lance/include/ch_lance.h` 的 `ch_lance_scan_options` 增加字段（保持 ABI 由同一仓库同步编译，无稳定 ABI 约束，但 Rust/C++ 必须同改）：

```c
typedef struct ch_lance_scan_options
{
    uint64_t version;
    ch_lance_string_list projection;
    const char * predicate;
    bool need_only_count;
    uint64_t max_block_size;
    uint64_t limit;
    ch_lance_cancel_handle * cancel;

    /// true: deterministic fragment order (default). false: allows fragment_readahead.
    bool scan_in_order;
    /// 0 = leave SDK default; >0 → Scanner::fragment_readahead
    uint32_t fragment_readahead;
    /// 0 = leave SDK default; >0 → Scanner::batch_readahead
    uint32_t batch_readahead;
    /// 0 = leave SDK default; >0 → Scanner::io_buffer_size
    uint64_t io_buffer_size;
} ch_lance_scan_options;
```

### 3.2 Rust `ch_lance_plan_scan`

在现有 `dataset.scan()` 配置链中，`try_into_stream` 之前：

```text
scanner.scan_in_order(options.scan_in_order);
if fragment_readahead > 0: scanner.fragment_readahead(n)
if batch_readahead > 0: scanner.batch_readahead(n)
if io_buffer_size > 0: scanner.io_buffer_size(n)
```

文档注释写明：`fragment_readahead` 仅在 `scan_in_order=false` 时由 SDK 使用。

### 3.3 C++ `ScanDescription` / `DatasetHandle::planScan`

扩展 `LanceScanDescription.h`：

```cpp
bool scan_in_order = true;
UInt32 fragment_readahead = 0;
UInt32 batch_readahead = 0;
UInt64 io_buffer_size = 0;
```

`LanceWrapper.cpp` 填入 `ch_lance_scan_options`。

### 3.4 Settings（`src/Core/Settings.cpp`）

| Setting | 类型 | 默认 | 含义 |
|---------|------|------|------|
| `lance_scan_in_order` | Bool | `true` | 映射 `scan_in_order` |
| `lance_fragment_readahead` | UInt64 | `0` | 0=SDK 默认 |
| `lance_batch_readahead` | UInt64 | `0` | 0=SDK 默认 |
| `lance_io_buffer_size` | UInt64 | `0` | 0=SDK 默认 |

从 `LanceMetadata::read` / `ReadSource` 构造 `ScanDescription` 时读取 `Context` settings。

### 3.5 ProfileEvents（可选但推荐）

- `LanceScanInOrder` — 若本次 scan `scan_in_order==true` 则 +1（或记录 0/1 语义用两个 counter）
- `LanceFragmentReadaheadConfigured` — 写入配置值（或仅 debug log）

若为减小 diff，T1 可只打 debug log，T3 统一补 counters。

---

## 4. 改动文件清单

| 文件 | 动作 |
|------|------|
| `rust/workspace/lance/include/ch_lance.h` | 扩展 `ch_lance_scan_options` |
| `rust/workspace/lance/src/lib.rs` | `plan_scan` 应用参数；单元测试 |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h` | 新字段 |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.cpp` | 填 FFI |
| `src/Storages/ObjectStorage/DataLakes/Lance/LanceMetadata.cpp` 和/或 `LanceReadSource.cpp` | 读 settings |
| `src/Core/Settings.cpp` | 声明 settings |
| `docs/en/engines/table-engines/integrations/lance.md` | Observability/limitations 旁加 settings 说明（一句即可，完整调参见 T5） |
| 测试 | 见 §6 |

---

## 5. 实现注意

- 现有调用点凡构造 `ch_lance_scan_options` / `ScanDescription` 的，必须 **零初始化或显式默认**（`scan_in_order=true`，其余 0），避免栈垃圾。
- Rust 侧 gtest/集成：`scan_in_order=false` + `fragment_readahead>0` 至少跑通一次全表 scan（行数不变）。
- **不要**在本任务改 `LanceDatasetIterator` 或 `estimatedKeysCount`。
- 与 T2 并行时：若 T2 也改 `ch_lance_scan_options`，合并时注意字段顺序一致；建议 T1 先合并或约定字段追加在 struct 尾部。

---

## 6. 测试与验证

### 必做

1. Rust 单元测试（`lib.rs` tests 模块）：默认 options 与 `scan_in_order=false`+readahead 全表行数一致。  
2. 现有 local stateless Lance 测试全绿（至少 `04101`, `04549`–`04555`, `04626`, `04627`）。  
3. 手动或 SQL：`SET lance_scan_in_order=0, lance_fragment_readahead=4` 后 `SELECT count()` / checksum 与默认一致。

### 验证命令（示例）

```bash
cd build && ninja clickhouse unit_tests_dbms > build_lance_t1.log 2>&1
# Rust tests via existing lance crate test path if wired; else cargo test in rust/workspace for _ch_rust_lance
./tests/clickhouse-test 04101_lance 04549_lance 04550_lance 04551_lance 04626_lance 04627_lance
```

---

## 7. 完成定义（DoD）

- [ ] FFI/C++/Settings 贯通，默认行为与合并前一致  
- [ ] `lance_scan_in_order=0` + readahead 结果正确  
- [ ] 相关测试通过  
- [ ] 文档至少提到新 settings  
- [ ] 本文件 Status → `done`；`README.md` 状态表更新  

---

## 8. 风险

| 风险 | 缓解 |
|------|------|
| 无序 scan 破坏依赖顺序的测试 | 默认 `scan_in_order=true` |
| 与 T2 同时改 options struct | 字段只追加；合并时对齐 |
| readahead 过大内存 | 默认 0 用 SDK 默认；文档提示 |

---

## 9. 回滚

用户侧：保持默认 settings，或显式：

```sql
SET lance_scan_in_order = 1;
SET lance_fragment_readahead = 0;
SET lance_batch_readahead = 0;
SET lance_io_buffer_size = 0;
```
