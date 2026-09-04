# T1 Lance Correctness and Process-Safety Implementation Report

## 14.1 Outcome

本次实现关闭了两个 P0 风险：

- `Lance` 查询不再只用 `version` 固定 snapshot。`TableStateSnapshot` 现在包含 `version`、由 canonical manifest path 和 naming scheme 计算的 `manifest_id`、`manifest_size`、raw manifest bytes 的 `manifest_sha256`、`has_etag` 和可选的 `etag_sha256`。`version`、size 和两个 mandatory digest 必须非零；`has_etag` 与 `etag_sha256` 的 zero/non-zero 状态必须一致。
- 所有 snapshot-sensitive 读取在 Rust 中执行 `checkout_exact_snapshot` 并比较完整 identity。相同 URI 被删除并以相同 `version` 重建时，schema、row count、fragment list 和 scan planning 均返回稳定的 snapshot mismatch，不读取新 dataset，也不回退到 latest。
- predicate translator 只能接收 pinned physical schema 中精确匹配的顶层字段。虚拟列、计算结果、nested path、cast result、未知字段和类型不一致字段均保留给 ClickHouse residual filter。
- predicate 白名单为：
  - nullable、已支持物理类型的 `isNull` 和 `isNotNull`；
  - non-nullable `Int8`、`Int16`、`Int32`、`Int64` 的 `equals`、`notEquals`、`less`、`lessOrEquals`、`greater`、`greaterOrEquals`；
  - Arrow UTF-8、non-nullable `String` 的 `equals` 和 `notEquals`；
  - 上述 signed integer 或 `String` 的单列、有限、无 `NULL`、精确类型 `IN`。
- `UInt*`、float、date/time、`FixedString`、`Decimal`、nullable comparison/`IN`、含 `NULL` 或空的 `IN`、cast、nested/complex 类型和未列出的函数均 fail-close，不生成 `Lance` predicate。partial `AND` 只下推安全原子；`OR` 为 all-or-nothing；完整 ClickHouse residual filter 始终保留。
- 19 个 Rust `extern "C"` export 均通过统一的 unwind guard。bool、pointer 和 void/free 返回类在 panic 后返回安全值，重置 output，生成 bounded internal error（有 error channel 时），且不让 panic 穿过 C ABI。
- `Lance` 的 `supportsPrewhere` 保持 `false`。显式 `PREWHERE` 被稳定拒绝为 `ILLEGAL_PREWHERE`；row policy 由 ClickHouse pipeline 正确执行，没有 RLS predicate 下推。

## 14.2 Files Changed

### C++ snapshot、query 和 read path

- `src/Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h`
  - 定义完整 immutable manifest identity 和逐字段 equality。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.cpp`
  - 增加 identity invariant validation、v2 serialization，以及 legacy version-only payload 的 fail-close。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h`
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.cpp`
  - 用 `pinSnapshot` 和完整 identity conflict detection 取代 version-only pin。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h`
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.cpp`
  - C++ wrapper 的 schema/count/list/scan API 改为完整 snapshot；映射专用 snapshot mismatch error 并增加 ProfileEvent。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceMetadata.cpp`
  - 在同一 pinned snapshot 上获取 physical schema；实现 schema allowlist、语义白名单、kill switch、completeness 和 defensive `PREWHERE`/RLS handling。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.h`
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.cpp`
  - cluster task 携带完整 snapshot，并验证 protocol、fragment count 和 pack invariant。
- `src/Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.cpp`
  - 修正 constructor initializer 顺序，使 release build 的 `-Wreorder-ctor` 检查通过。
- `src/Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h`
  - 暴露 compile-time `SUPPORTS_PREWHERE` capability 供测试；`LanceMetadata` specialization 仍为 `false`。

### Rust / FFI

- `rust/workspace/lance/include/ch_lance.h`
  - 增加 `ch_lance_snapshot_info` identity fields 和 `CH_LANCE_ERROR_SNAPSHOT_MISMATCH`；snapshot-sensitive function signatures 改为完整 snapshot。
- `rust/workspace/lance/src/lib.rs`
  - 从 canonical manifest location 和 raw manifest bytes 计算 SHA-256 identity；
  - 实现 `checkout_exact_snapshot`；
  - 对 schema/count/list/scan 执行完整 identity validation；
  - 对全部 19 个 export 增加 panic guard 和 output reset；
  - 增加 snapshot、same-path recreation、panic return class、ownership 和 error mapping tests。
- `rust/workspace/lance/Cargo.toml`
  - 增加直接 `sha2` dependency。
- `rust/workspace/Cargo.toml`
  - 明确 release profile 使用 `panic = "unwind"`。
- `rust/workspace/Cargo.lock`
  - 同步实际 dependency graph。

### Protocol、settings 和 events

- `src/Core/ProtocolDefines.h`
  - data-lake snapshot protocol 从 1 升到 2；
  - cluster processing protocol 增加 `DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_SNAPSHOT_IDENTITY = 10`。
- `src/Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.cpp`
  - 使用 v2 snapshot protocol。
- `src/Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.cpp`
  - 在全局 v2 envelope 下保持原有 `Iceberg` payload compatibility。
- `src/Core/Settings.cpp`
- `src/Core/SettingsChangesHistory.cpp`
  - 增加默认启用的 `lance_enable_predicate_pushdown`。
- `src/Common/ProfileEvents.cpp`
  - 增加 `LanceSnapshotIdentityMismatch` 和 `LancePredicatePushdownDisabled`。

### Tests

- `src/Storages/ObjectStorage/DataLakes/Lance/tests/gtest_lance_table_state_snapshot.cpp`
  - 覆盖完整 round-trip、truncation、zero/invalid identity 和 legacy rejection。
- `src/Storages/ObjectStorage/DataLakes/Lance/tests/gtest_lance_query_session.cpp`
  - 覆盖 same-version/different-digest pin conflict。
- `src/Storages/ObjectStorage/tests/gtest_datalake_table_state_serde.cpp`
  - 覆盖 `Iceberg` compatibility、完整 `Lance` cluster payload、old protocol rejection 和 pack validation。
- `tests/queries/0_stateless/data_lance/sql/04627_lance_local_profile_events.sql`
  - 使用 fixture 的 nullable schema 修正 complete/partial event assertions。
- `tests/queries/0_stateless/04630_lance_local_snapshot_identity.sh`
- `tests/queries/0_stateless/04630_lance_local_snapshot_identity.reference`
  - 在 query failpoint 中删除并同路径重建 dataset，要求 deterministic snapshot mismatch。
- `tests/queries/0_stateless/04631_lance_local_predicate_semantics.sh`
- `tests/queries/0_stateless/04631_lance_local_predicate_semantics.reference`
- `tests/queries/0_stateless/data_lance/sql/04631_lance_local_predicate_semantics.sql`
  - pushdown on/off differential、partial `AND`、strict `OR`、nullable/date/`IN NULL` rejection 和 disabled event。
- `tests/queries/0_stateless/04632_lance_local_virtual_predicate.sh`
- `tests/queries/0_stateless/04632_lance_local_virtual_predicate.reference`
- `tests/queries/0_stateless/data_lance/sql/04632_lance_local_virtual_predicate.sql`
  - 覆盖 `_data_lake_snapshot_version`、`_path` 和 `_file`。
- `tests/queries/0_stateless/04633_lance_local_prewhere_rls.sh`
- `tests/queries/0_stateless/04633_lance_local_prewhere_rls.reference`
- `tests/queries/0_stateless/data_lance/sql/04633_lance_local_prewhere_rls.sql`
  - 断言显式 `PREWHERE` 返回 `ILLEGAL_PREWHERE`，并验证 row policy 结果为 `[1, 3]`。

`contrib/rust_vendor` 和 `rust/workspace/CMakeLists.txt` 在任务开始前已有用户修改，本次交付未把它们作为实现变更提交。

## 14.3 Contract Changes

### C++ API

- `Lance::TableStateSnapshot` 从单一 `UInt64 version` 扩展为完整 immutable manifest identity。
- `DatasetHandle` 的 `tableSchema`、`totalRows`、`countRows`、`listFragments` 和 `planScan` 接收完整 snapshot。
- `QuerySession::pinSnapshot` 比较全部 identity fields；同一 identity key 的任一字段冲突均抛出异常。
- `LanceObjectSerializableInfo` 携带完整 snapshot、fragment IDs、pack index 和 pack count。

### C ABI

- `ch_lance_snapshot_info` 增加两个固定 32-byte mandatory digest、manifest size、`has_etag` 和固定 32-byte optional eTag digest。
- `ch_lance_scan_options` 内嵌完整 snapshot。
- schema/count/list operations 从 version 参数改为 `const ch_lance_snapshot_info *`。
- 新增稳定 error kind `CH_LANCE_ERROR_SNAPSHOT_MISMATCH = 11`。
- export symbol 数保持 19；未增加 Rust-owned snapshot string 或新 free protocol。

### Serialized / cluster protocol

- `DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION = 2`。
- `DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION = 10`，新 capability 为 `DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_SNAPSHOT_IDENTITY`。
- version-only `Lance` snapshot 和 protocol `< 10` 的 `Lance` cluster task 均明确拒绝。
- `Iceberg` 等其他 snapshot variant 在 v2 envelope 下保留原 payload compatibility。

### Settings / ProfileEvents

- `lance_enable_predicate_pushdown`：`Bool`，默认 `true`；只控制已验证白名单。`false` 时完整 ClickHouse filter 保留，且不生成 `Lance` predicate。
- `LanceSnapshotIdentityMismatch`：完整 identity 校验失败时增加。
- `LancePredicatePushdownDisabled`：存在 filter 且 setting 显式关闭时增加。
- 原有 complete/partial/limit events 按严格 completeness 约束。

### Mixed-version behavior

新 coordinator 不会向 cluster protocol 9 或更旧 worker 分发 `Lance` identity task。不存在 version-only downgrade、latest retry 或缺失 identity wildcard。

## 14.4 Test Evidence

### Static contract audit

Command:

```bash
rg -n '^pub unsafe extern "C" fn' rust/workspace/lance/src/lib.rs
rg -n 'checkout_exact_version|pinVersion|getPinnedVersion' rust/workspace/lance/src src/Storages/ObjectStorage/DataLakes/Lance
rg -n 'supportsPrewhere' src/Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h
git diff --check
```

- Exit: 0
- Log: `/data/develop/lance-format/build_lance_mvp/test_t1_static_checks.log`
- Result: 19 exports found; no legacy version-only production names; `supportsPrewhere` path present; `git diff --check` clean.

### Rust unit tests

Command:

```bash
cargo test --manifest-path rust/workspace/lance/Cargo.toml
```

- Exit: 0
- Log: `/data/develop/lance-format/build_lance_mvp/test_t1_rust_lance_8.log`
- Count: 36 executed, 36 passed, 0 failed, 0 ignored/skipped.
- Subagent summary: no warning/error; `ffi_rejects_same_path_same_version_recreated_dataset`, `snapshot_identity_is_stable_for_local_manifest` and `panic_guards_keep_all_ffi_return_classes_inside_rust` all ran and passed.

### Release build

Command:

```bash
ninja -C build_lance_mvp clickhouse unit_tests_dbms
```

The three pre-existing untracked contrib directories were temporarily moved under `tmp/t1_hidden_contrib_build4` during CMake regeneration and restored by a shell trap.

- Exit: 0
- Log: `/data/develop/lance-format/build_lance_mvp/build_t1_4.log`
- Targets: 7374 build edges; final `programs/clickhouse` and `src/unit_tests_dbms` linked.
- Subagent summary: changed `Lance` C++ units and release `_ch_rust_lance` rebuilt; no compiler warning/error. The only warning was Ninja state-file recovery: `premature end of file; recovering`.

### C++ unit tests

Command:

```bash
build_lance_mvp/src/unit_tests_dbms \
  --gtest_filter='LanceQuerySession.*:LanceTableStateSnapshot.*:LanceConfiguration.*:LanceWrapper.*:DatalakeStateSerde.*'
```

- Exit: 0
- Log: `/data/develop/lance-format/build_lance_mvp/test_t1_cpp_unit_2.log`
- Count: 24 executed from 5 suites, 24 passed, 0 failed, 0 skipped.
- Subagent summary: seven expected `LOGICAL_ERROR` exception traces came from passing negative-path tests; no warning or unexpected error.
- Coverage includes full snapshot serde/invariants, legacy rejection, `Iceberg` compatibility, cluster payload/pack validation, query pin conflict, error mapping and `supportsPrewhere=false`.

### Lance stateless tests

The tests ran against the newly built `26.7.1.1` server on isolated TCP/HTTP ports with `--no-random-settings`.

Command:

```bash
CLICKHOUSE_HOST=127.0.0.1 \
CLICKHOUSE_PORT_TCP=19000 \
CLICKHOUSE_PORT_HTTP=18123 \
CLICKHOUSE_PORT_HTTP_PROTO=http \
CLICKHOUSE_CONFIG=/data/develop/lance-format/tmp/t1_server2/preprocessed_configs/config.xml \
CLICKHOUSE_USER_FILES=/data/develop/lance-format/tmp/t1_server2/user_files \
./tests/clickhouse-test \
  -b build_lance_mvp/programs/clickhouse \
  --configclient tmp/t1_client_config.xml \
  --configserver tmp/t1_server2/preprocessed_configs/config.xml \
  --no-random-settings \
  04549_lance_local_predicate_pushdown \
  04551_lance_local_count_pushdown \
  04626_lance_local_limit_pushdown \
  04627_lance_local_profile_events \
  04630_lance_local_snapshot_identity \
  04631_lance_local_predicate_semantics \
  04632_lance_local_virtual_predicate \
  04633_lance_local_prewhere_rls
```

- Exit: 0
- Log: `/data/develop/lance-format/build_lance_mvp/test_t1_lance_final_2.log`
- Count: 8 executed, 8 passed, 0 failed, 0 skipped.
- Subagent summary: all eight selected tests passed; runner split was 7/7 parallel and 1/1 sequential.

### Required focused evidence

- Same-path/same-version recreation:
  - Rust `ffi_rejects_same_path_same_version_recreated_dataset` verifies schema, total/count rows, fragment list and scan planning all return `SnapshotMismatch`.
  - Stateless `04630_lance_local_snapshot_identity` pauses a live query, replaces the path with another version-1 dataset, and passes only when the query reports snapshot identity mismatch.
- Virtual columns:
  - `04632_lance_local_virtual_predicate` filters on `_data_lake_snapshot_version`, `_path` and `_file`; all three queries return the expected four rows through residual filtering.
- Pushdown differential:
  - `04631_lance_local_predicate_semantics` compares pushdown enabled and disabled results for signed integer range, quoted UTF-8 string, nullable comparison, date, `IN` with `NULL`, partial `AND` and incomplete `OR`; it also verifies `LancePredicatePushdownDisabled`.
  - Existing `04549`, `04551`, `04626` and `04627` verify predicate, count, limit and profile-event behavior.
- Panic injection:
  - Rust `panic_guards_keep_all_ffi_return_classes_inside_rust` injects panic through bool, pointer and void/free wrappers; the process continues and safe return/output states are asserted.
  - Release `_ch_rust_lance` rebuilt with `panic=unwind`; a compile-time guard rejects any other panic strategy.
- `PREWHERE` / row policy:
  - `04633_lance_local_prewhere_rls` confirms explicit `PREWHERE` is rejected with `ILLEGAL_PREWHERE`, then confirms row policy `id IN (1, 3)` produces exactly `[1, 3]`.
  - `LanceConfiguration.PrewhereRemainsDisabled` independently locks the capability to `false`.

## 14.5 Acceptance Checklist

- [x] `TableStateSnapshot` 包含 version、manifest ID、size、SHA-256 和 optional `e_tag` digest。
- [x] snapshot equality、serialization 和 cluster task payload 覆盖所有 identity 字段。
- [x] 所有 snapshot-sensitive FFI 调用使用完整 identity。
- [x] production code 不存在 version-only checkout。
- [x] 同路径删除重建、相同 version 的旧 snapshot 在 schema/count/list/scan 四条路径全部失败。
- [x] snapshot mismatch 增加 `LanceSnapshotIdentityMismatch`，且错误消息不泄露 credentials。
- [x] old version-only `Lance` payload fail-close。
- [x] 其他 data-lake snapshot variant serialization tests 通过。
- [x] `QuerySession` pin 完整 snapshot，same-version/different-digest 被拒绝。
- [x] translator 只有在提供 pinned physical schema allowlist 时才能运行。
- [x] 虚拟列和非物理输入从不进入 `Lance` predicate。
- [x] 类型/operator 白名单与 KD-6 完全一致。
- [x] unsigned、float、NaN/Inf、nullable comparison、date/time/timezone 默认不下推。
- [x] `IN` 含 `NULL` 时整个原子不下推。
- [x] partial `AND` 和 strict `OR` 行为通过测试。
- [x] predicate incomplete 时 count 和 limit fast path 不启用。
- [x] `lance_enable_predicate_pushdown=false` 时结果正确且不生成 `Lance` predicate。
- [x] pushdown on/off 差分测试结果完全一致。
- [x] 19 个 Rust `extern "C"` 入口均使用 panic guard。
- [x] bool、pointer、void/free 三类 panic injection tests 均证明进程继续运行。
- [x] 生产构建明确使用可被 `catch_unwind` 捕获的 panic strategy。
- [x] `Lance` 的 `supportsPrewhere` 仍为 false。
- [x] 显式 `PREWHERE` 和 row policy 回归测试通过。
- [x] Rust unit tests、目标 C++ unit tests、已有 `Lance` stateless tests 和新增 tests 全部通过。
- [x] build/test 输出均写入 build 目录中的唯一日志文件。
- [x] `no-fasttest` 仅用于需要 Rust-enabled build 的四个新测试；`04630` 的额外 `no-parallel` 严格用于全局 failpoint 隔离，且两项均有 test-header 说明。

## 14.6 Deviations

1. 计划中的 workspace command `cargo test --manifest-path rust/workspace/Cargo.toml -p _ch_rust_lance` 不可用，因为 `_ch_rust_lance` 不在该 workspace 的 `members` 中。实际使用 crate manifest `rust/workspace/lance/Cargo.toml`，运行相同 crate 的全部 36 个 tests。Correctness 影响：无。
2. 未增加 test-only C ABI symbol 供 C++ binary 主动触发 Rust panic。这样避免扩大生产 19-symbol ABI；实际 export wrappers 在 Rust tests 中直接注入并覆盖 bool/pointer/void/free，C++ tests 另行验证 error-kind mapping。Correctness 影响：panic boundary 本身被测试，但缺少一次从 C++ 调用栈进入注入 hook 的 smoke coverage；评审时应明确接受这一 coverage trade-off。
3. 新 stateless semantics test 使用现有 fixture 覆盖代表性的 supported/rejected expressions，但没有逐值生成计划中列出的全部 `UInt64` extreme、NaN/Inf/-0 和 DST gap/fold 数据。实现通过精确 type allowlist 在进入 literal serialization 前统一拒绝这些类型，且 date/nullable/`IN NULL` rejection 有 differential coverage。Correctness 影响：无已知语义缺口；存在未逐值枚举的测试覆盖风险。
4. 因 `supportsPrewhere=false`，显式 `PREWHERE` 的正确 contract 是稳定返回 `ILLEGAL_PREWHERE`，而不是返回结果集。RLS 正确结果由同一 stateless test 的独立 row-policy query 验证。Correctness 影响：无；这与 non-goal 和 capability contract 一致。

## 14.7 Residual Risks

- 每次完整 snapshot identity acquisition/validation 需要读取 raw manifest bytes；远程 object store 可能增加 I/O 和 latency。这不是 correctness fallback，失败会直接传播。
- mixed-version cluster 必须整体升级到 protocol 10 才能执行分布式 `Lance` task；旧 worker 会被明确拒绝。
- object-store data file 被原地覆写不在本 snapshot guarantee 范围。实现保证 immutable manifest identity，不计算 data-file content hash、object version tree 或 Merkle root。
- panic guard 可以保证 unwind 不跨 C ABI；如果底层依赖直接 abort，语言级 `catch_unwind` 无法拦截。production profile 和 compile-time check 已保证 Rust panic strategy 为 unwind。
- predicate matrix 的保守性会降低 pushdown 命中率；这是预期 correctness trade-off，不存在自动 fallback 到更宽 translator。

## 14.8 Git and PR

- Branch: `t1-lance-correctness-safety`
- Commits: implementation commit and this report commit are listed in the final delivery summary.
- History policy: no amend and no rebase were used。
- PR: not created; no external PR mutation was requested。
- Required PR base: `master`。
- Required PR body: `.github/PULL_REQUEST_TEMPLATE.md`，保留模板的 description/motivation、单一 Changelog category 和 Changelog entry structure。
- CI URLs: none were provided during this task。
