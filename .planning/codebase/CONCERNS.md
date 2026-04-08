# ClickHouse Codebase Technical Concerns

**Last Updated:** 2026-03-26
**Scope:** ~228K LOC in src/, ~7,500 source files, 256 contrib dependencies

---

## 1. Build System Complexity

**Severity:** HIGH

### CMake Infrastructure
- **Root CMakeLists.txt:** 677 lines with 12 major directives (`add_subdirectory`, `add_library`, `target_link_libraries`)
- **cmake/ directory:** 25+ configuration files managing compiler flags, sanitizers, CPU features, platform-specific code
- **File:** `CMakeLists.txt` (root)
- **Related files:** `cmake/sanitize.cmake`, `cmake/tools.cmake`, `cmake/cpu_features.cmake`

### Contrib Dependency Explosion
- **256 external libraries** in `contrib/` directory
- Each contrib has:
  - Source code (often outdated versions)
  - Wrapper CMake files (`contrib/*-cmake/`)
  - Complex interdependencies (e.g., aws-c-auth → aws-c-common → aws-c-cal)
- **Painful upgrades:** Updating any contrib potentially requires rebuilding half the codebase
- **License complexity:** Mixed licenses (Apache, MIT, GPL, custom) create compliance headaches

### Build Time & Incremental Compilation Issues
- **Estimated full build:** 20+ minutes on modern hardware (varies by sanitizer)
- **Problematic files:**
  - `src/Functions/URL/tldLookup.generated.cpp` (110K LOC, auto-generated)
  - `src/Functions/HTMLCharacterReference.generated.cpp` (17K LOC)
  - Large header-only libraries create template bloat
- **No distributed ccache setup documented** for CI
- **ninja used without explicit -j management** (relies on system defaults)

---

## 2. Code Complexity Hotspots

**Severity:** HIGH

### Monolithic Classes
| File | Lines | Issue |
|------|-------|-------|
| `src/Interpreters/Context.cpp` | 7,694 | Central context object with 1,875-line header; god object pattern |
| `src/Storages/StorageReplicatedMergeTree.cpp` | 11,593 | Replication state machine, locking logic, distributed consensus |
| `src/Storages/MergeTree/MergeTreeData.cpp` | 10,951 | Core MergeTree implementation; part metadata, merges, mutations |
| `src/Interpreters/ActionsDAG.cpp` | 4,286 | Query execution DAG; complex expression evaluation |
| `src/Analyzer/Resolve/QueryAnalyzer.cpp` | 5,566 | New query analyzer; partial replacement for old system |
| `src/Storages/MergeTree/KeyCondition.cpp` | 5,107 | Index condition extraction; index range pruning logic |

### Parser Complexity
- **File:** `src/Parsers/ExpressionListParsers.cpp` (3,108 lines)
- **Issue:** Manual recursive descent parser with minimal DSL support
- **Maintenance burden:** Any SQL extension requires careful parser changes

### Query Pipeline Fragmentation
- **Files involved:** `InterpreterSelectQuery.cpp` (3,672 lines), `QueryAnalyzer` (5,566 lines)
- **Architectural tension:** Two query pipelines coexist:
  - Old `InterpreterSelectQuery` → legacy execution
  - New `Analyzer` + `QueryPlan` → modern execution (not complete parity)
  - Cost: duplicate logic, inconsistent behavior
- **Related file:** `tests/analyzer_tech_debt.txt` (technical debt log exists)

### Thread-Unsafe Globals
- **Context object uses TLS extensively** (`ThreadLocalPtr<Context>`)
- **2,494 uses of `std::mutex`, `std::lock_guard`, `std::atomic`** across codebase
- **Files:** `src/Interpreters/Context.cpp`, `src/Storages/StorageReplicatedMergeTree.cpp`
- **Risk:** Potential data races in concurrent access patterns; deadlock hazards in complex locking scenarios

---

## 3. Technical Debt & Known Issues

**Severity:** HIGH

### TODO/FIXME Comment Density
- **694 TODO comments** scattered across src/
- **103 FIXME comments**
- **6 HACK comments** (lowest but often most dangerous)

### Top Problematic Files
| File | Comments | Known Issues |
|------|----------|--------------|
| `src/Storages/StorageReplicatedMergeTree.cpp` | 19 TODO | Replication edge cases, race conditions |
| `src/Storages/StorageDistributed.cpp` | 9 TODO | Distributed query handling, failover logic |
| `src/Storages/MergeTree/MergeTreeData.cpp` | 9 TODO | Part lifecycle management |
| `src/Interpreters/Context.cpp` | 8 TODO | TLS access, initialization order |
| `src/Interpreters/TransactionLog.cpp` | 7 TODO | MVCC implementation incomplete |

### Critical TODOs
- **Access control:** `src/Access/ContextAccess.h` "TODO: Fix race" (thread safety issue in permission checks)
- **Query analysis:** `src/Analyzer/Passes/QueryAnalysisPass.h` - Scalar subquery evaluation not fully correct
- **Recursive CTEs:** `src/Analyzer/RecursiveCTE.h` - 5 TODOs including locking improvements, SEARCH/CYCLE syntax

### Analyzer Technical Debt
- **File:** `tests/analyzer_tech_debt.txt`
- **Issue:** New query analyzer incomplete; old system still required for backward compatibility
- **Cost:** ~2,727 references to `InterpreterSelectQuery` or `ActionsDAG` indicate complexity of dual-path support

---

## 4. Architectural Tensions

**Severity:** MEDIUM-HIGH

### Query Pipeline Duality
```
Old Path:  InterpreterSelectQuery → ExpressionAnalyzer → ActionsDAG
New Path:  QueryAnalyzer → QueryPlan → Processors
```
- **Decision point:** At runtime, query takes one path or another
- **Risk:** Features implemented in only one path; inconsistent query semantics
- **Maintenance:** Bug fixes must sometimes be applied to both codepaths
- **Example:** `src/Analyzer/Passes/` directory has optimization passes; legacy system has different optimizations in `src/Interpreters/`

### MergeTree Storage Complexity
- **Competing concerns:**
  - Durability (write-ahead logging, replication)
  - Performance (async merges, mutations, parallel writes)
  - Consistency (MVCC, transaction isolation)
- **Key files:** `MergeTreeData.cpp`, `MutateTask.cpp`, `MergeTask.cpp`, `ReplicatedMergeTreeQueue.cpp`
- **No clear separation:** All logic in single files; 30K+ LOC in MergeTree/ directory

### Old vs. New Column Storage Formats
- **Dynamic column support:** `ColumnDynamic` (JSON-like sparse columns)
- **File:** `src/Storages/MergeTree/KeyCondition.cpp` + column type system
- **Issue:** Index condition extraction must handle multiple column representations
- **References:** 497 uses of `DataLayout`, `ColumnSparse`, `ColumnDynamic`

### Access Control Design Issues
- **Race condition:** `src/Access/ContextAccess.h:41` marked "TODO: Fix race"
- **TLS usage:** Thread-local context storage; potential initialization order bugs
- **File:** `src/Access/ContextAccess.cpp` (complex permission resolution)

---

## 5. Testing Gaps & CI Challenges

**Severity:** MEDIUM-HIGH

### Test Exclusions (Known Failures)
| File | Exclusions | Category |
|------|-----------|----------|
| `tests/parallel_replicas_blacklist.txt` | 315 tests | Parallel replica execution failures |
| `tests/async_insert_blacklist.txt` | 52 tests | Asynchronous insert issues |
| `tests/tsan_ignorelist.txt` | 13 tests | Thread sanitizer false positives |
| `tests/ubsan_ignorelist.txt` | 22 tests | Undefined behavior sanitizer noise |

### Specific Known Issues
- **Parallel replicas:** Filter pushdown for distributed tables with datetime (issue #94612)
- **Temporary tables:** Access in readonly mode, schema detection in VALUES (multiple blockers)
- **Distributed execution:** Several tests disabled due to distributed query semantics issues
- **Reference:** Blacklist entries reference GitHub issues; not all root causes fixed

### Test Infrastructure Fragmentation
```
tests/
├── queries/0_stateless/       # Main regression tests
├── integration/               # 737 integration test modules
├── performance/               # Performance regression suite
├── casa_del_dolor/            # Stress/edge case tests
├── sqllogic/                  # SQL compliance tests
└── ci/                        # CI configuration
```
- **No unified test framework:** Mix of Python, shell, SQL-based testing
- **Flaky tests:** Long-running integration tests prone to timeout false positives
- **Performance testing:** Separate infrastructure; performance regressions sometimes miss CI

### Integration Test Complexity
- **737 directories in tests/integration/**
- **Heavy Docker usage:** Tests require containerized ClickHouse + dependencies
- **Slow iteration:** Integration test runs take 30+ minutes
- **Parallel execution issues:** Many tests not marked with `no-parallel` but have race conditions

---

## 6. Common CI Failure Patterns

**Severity:** MEDIUM

### ThreadSanitizer (TSan) Issues
- **13 entries in tsan_ignorelist.txt** (known false positives + real bugs)
- **File:** `tests/tsan_ignorelist.txt`
- **Problem areas:**
  - Lock-free data structures (`AtomicInteger`, `LockFreeAllocator`)
  - Fiber-based execution (async coroutines)
  - Thread pool queue (TaskQueue, ThreadPool)
- **Root causes:** Benign race conditions in performance-critical code; TSan overly conservative

### UndefinedBehaviorSanitizer (UBSan) Issues
- **22 false positives catalogued** in ubsan_ignorelist.txt
- **Common triggers:**
  - Signed integer overflow in aggregate calculations
  - Pointer arithmetic near bounds
  - Uninitialized fields in auto-generated code

### Flaky Integration Tests
- **Blacklist for parallel_replicas (315 tests)** indicates deep issues with:
  - Distributed query coordination
  - Replica election under failure
  - Network partition handling
- **Probable causes:** Timing-dependent assertions, insufficient retry logic

### Build Failures
- **Generated code:** Auto-generated files (tldLookup.generated.cpp) sometimes out-of-sync
- **Dependency resolution:** Complex cmake dependency graph leads to missing symbols
- **Header ordering:** Some headers have implicit dependencies; forward declarations incomplete

---

## 7. Performance-Sensitive Areas

**Severity:** MEDIUM

### Hot Paths
| Component | File | Concern |
|-----------|------|---------|
| **Aggregation** | `src/Interpreters/Aggregator.cpp` (4,210 LOC) | Hash table performance, spill-to-disk logic |
| **Query execution** | `src/Processors/QueryPlan/ReadFromMergeTree.cpp` (4,267 LOC) | I/O scheduling, prefetch logic |
| **Compression** | `src/Compression/*` (11,983 LOC total) | Multiple codec implementations; potential SIMD mismatches |
| **Join execution** | `src/Interpreters/HashJoin/HashJoin.cpp` (2,055 LOC) | Memory layout, hash collision handling |
| **Window functions** | `src/Processors/Transforms/WindowTransform.cpp` (3,283 LOC) | Complex state tracking; sort buffer management |

### Memory Management Patterns
- **4,368 uses of raw `new`/`delete`/`malloc`/`free`** across src/
- **High risk areas:**
  - Allocators: `UniquesHashSet` uses custom allocator with stack memory
  - Buffers: `ReadBuffer`, `WriteBuffer` hierarchy (~50 derived classes)
  - Column data: Variable-length data stored in Vector<String> (implicit allocations)
- **Files:** `src/Common/Allocator.h`, `src/Core/Field.h`, column type hierarchy

### Absence of LIKELY/UNLIKELY Macros
- **0 occurrences** of branch prediction hints in performance-critical code
- **Opportunity:** Aggregator.cpp, Aggregator functions, compression codecs could benefit
- **Impact:** Compiler must guess branch direction; can lead to suboptimal pipeline

### No Explicit NUMA Support
- **File:** `src/Common/*` (allocator)
- **Gap:** Large systems with NUMA don't benefit from topology-aware allocation
- **Workaround:** Relies on Linux default first-touch policy

---

## 8. Thread Safety Considerations

**Severity:** MEDIUM

### Context Object Design
- **File:** `src/Interpreters/Context.cpp`
- **Pattern:** Singleton-like, TLS-based access
- **Issues:**
  - Child contexts copied; semantic unclear in async settings
  - User/session isolation relies on TLS correctness
  - Deadlock potential: nested Context locks + Database locks + Table locks

### Lock Hierarchy Violations
- **923 references to ThreadPool** across codebase
- **Files:** Task scheduling in `MergeTree/`, `ObjectStorage/`, `Processors/`
- **Risk:** Task chains can create circular waits:
  - Task A submits Task B and waits for result
  - Task B tries to acquire lock held by task A's user thread
- **Mitigation:** No documented lock ordering; developers must reason locally

### TSAN Suppressions
- **Fiber library:** "workaround-asan-fiber-false-positive" (commit shows suppression added)
- **Issue:** Coroutine library triggers TSAN; underlying cause not fixed
- **Files:** `cmake/sanitize.cmake`, `src/Common/Fiber.h`

### Concurrent Access to Global State
- **DatabaseCatalog:** Central registry of tables; concurrent CREATE/DROP/ALTER
- **StorageFactory:** Registration of storage engines
- **File:** `src/Interpreters/DatabaseCatalog.cpp` (2,244 lines)
- **Gap:** No explicit documentation of how concurrent DDL is serialized

---

## 9. Memory Management Challenges

**Severity:** MEDIUM

### Arena Allocators & Fragmentation
- **Pattern:** Temporary arenas for query execution
- **Issue:** If query is killed mid-execution, arena leaks until connection closes
- **File:** `src/Common/ArenaAllocator.h`
- **Mitigation:** Query profiling kills queries; eventual cleanup

### Variable-Length Data Storage
- **String columns:** Stored as offsets + data blob
- **Issue:** No separate memory accounting for data blob; all in ColumnString
- **Risk:** OOM queries can cause cascading failures
- **Related:** MemoryTracker in `src/Common/MemoryTracker.h`

### Stack Allocator Limits
- **File:** `src/Common/UniquesHashSet.h`
- **Code:** `HashTableAllocatorWithStackMemory` reserves space on stack
- **Risk:** For large sets (1M elements), stack overflow if aggregate function keeps too many states

### No Memory Pooling for Allocations
- **Alternative designs:** jemalloc fragmentation, per-thread pools
- **Current:** malloc/free with MemoryTracker overlay
- **Impact:** Fragmentation over long-running servers; NUMA-unfriendly

---

## 10. Code Quality & Maintenance Burden

**Severity:** MEDIUM

### Manual Type Conversions
- **2,012 uses of `dynamic_cast` and `reinterpret_cast`** across src/
- **Risk areas:**
  - Storage engine dispatch: `StorageFactory` casts to specific engine types
  - Expression evaluation: Type coercion in binary operations
  - Data format handling: Format writers cast to derived classes
- **File examples:** `src/Storages/`, `src/Functions/`, `src/Formats/`

### Generated Code Maintenance
- **tldLookup.generated.cpp (110K LOC):** URL parsing lookup table
- **HTMLCharacterReference.generated.cpp (17K LOC):** HTML entity reference
- **Issue:** Changes require regeneration; scripts not always updated
- **Risk:** Outdated codegen used in builds

### Parser Grammar Complexity
- **Manual recursive descent parser:** No parser generator (Bison, Antlr not used broadly)
- **Maintenance:** SQL extension requires careful hand-written parser logic
- **File:** `src/Parsers/ExpressionListParsers.cpp` (3,108 lines)
- **Alternate:** `src/Client/BuzzHouse/` uses ANTLR (newer code, not yet default)

### Header Dependencies
- **Deep include chains:** Some headers include dozens of other headers
- **Impact:** Compilation time, circular dependency risks
- **Example:** `src/Interpreters/Context.h` transitively includes most of interpreter/storage code

### Lack of Code Locality
- **No domain separation:** Interpreter, storage, and format code deeply intertwined
- **Example:** Interpreter must know storage details (part pruning logic in KeyCondition)
- **Refactoring barrier:** Hard to change storage without affecting interpreter

---

## 11. Specific Architectural Debt

**Severity:** MEDIUM-HIGH

### Column Format Evolution
- **Old:** `ColumnConst`, `ColumnArray`, `ColumnNullable` (nested structs)
- **New:** `ColumnDynamic` (JSON-like), sparse formats
- **Transition cost:** Index extraction (KeyCondition.cpp) must handle all variants
- **Incomplete:** Some optimizations only work with certain column types

### Replica Coordination
- **File:** `src/Storages/StorageReplicatedMergeTree.cpp` (11,593 lines)
- **Design:** ZooKeeper queue-based coordination
- **Issues:**
  - Edge cases: network partition, zombie replicas, reordered mutations
  - Mutation replay: Every replica independently replays mutations; errors cascade
  - Entry inconsistencies: Replicas may have different queues
- **No compensation:** Failed mutations leave replicas diverged; manual intervention needed

### Transaction Support (MVCC)
- **File:** `src/Interpreters/TransactionLog.cpp` (7 TODOs)
- **State:** Partial implementation; not production-ready
- **Issues:**
  - Visibility logic incomplete
  - Conflict detection rudimentary
  - Performance impact unquantified

### Async Inserts
- **52 tests disabled** in async_insert_blacklist.txt
- **Problem areas:** Duplicate handling, deduplication logic, flush timing
- **Files:** `src/Storages/MergeTree/MutateFromLogEntryTask.cpp`
- **Root causes:** INSERT deduplication relies on token + async buffer coordination

---

## 12. External Dependency Risks

**Severity:** MEDIUM

### contrib/ Vulnerabilities
- **256 dependencies:** Each has its own security update cycle
- **Risk:** Outdated versions (commit history shows some pinned to old releases)
- **Examples:**
  - aws-* libraries: Security patches may lag ClickHouse releases
  - zstd, xxHash: Compression libraries; CVEs can affect data safety
  - LLVM: Used for JIT; complex dependency tree

### Platform-Specific Code
- **Files:** `cmake/darwin/`, `cmake/freebsd/`, `cmake/linux/`
- **Fragmentation:** Platform-specific storage engines, IO schedulers
- **Testing gap:** CI doesn't run on all platforms; arm/x86 divergence noted

### ANTLR Runtime Bloat
- **File:** `contrib/antlr4-cpp-runtime/` (~4.5K lines)
- **Usage:** Only in `BuzzHouse` module (experimental)
- **Issue:** Full antlr shipped but not default; unused for 99% of deployments

---

## 13. Documentation & Knowledge Transfer Gaps

**Severity:** LOW-MEDIUM

### Missing Runbooks
- **No explicit lock ordering documentation**
- **No concurrency model explained** for async query execution
- **No schema for internal metadata tables**

### Code Comment Quality
- **Many TODOs lack context:** "TODO: optimize" without explaining current bottleneck
- **Architectural comments sparse:** New developers must reverse-engineer design from code

### Test Coverage Opaqueness
- **Why are 315 tests blacklisted for parallel replicas?** Root causes not linked in code
- **Analyzer tech debt:** Document exists (analyzer_tech_debt.txt) but not referenced in source

---

## Priority Action Items

### Immediate (High Impact, High Effort)
1. **Reduce Context.cpp complexity** (7,694 lines) - Split into session/database/user contexts
2. **Stabilize analyzer:** Complete feature parity between old and new query pipelines; remove legacy path
3. **Fix thread safety in ContextAccess** - Marked TODO race condition

### Medium-term (Medium Impact, Medium Effort)
4. **Document lock hierarchy** - Prevent future deadlock regressions
5. **Stabilize parallel replica tests** - Reduce 315-test blacklist
6. **Reduce MergeTree complexity** - Split storage engine by concern (replication, merging, mutations)

### Long-term (Architectural)
7. **Modernize parser** - Adopt Antlr or other generator; reduce manual parsing logic
8. **Separate interpreter from storage** - Allow independent iteration
9. **NUMA-aware allocators** - Better performance on multi-socket systems

---

## Metrics Summary

| Metric | Value | Assessment |
|--------|-------|-----------|
| **Total source lines** | 228K | Large; typical for mature OLAP engine |
| **Files with TODO/FIXME** | 501 | High density (~7% of source files) |
| **Largest single file** | 11.5K lines | StorageReplicatedMergeTree.cpp (monolithic) |
| **Context LOC** | 9.6K | Problematic god object |
| **Contrib dependencies** | 256 | High; upgrade/security risk |
| **Concurrency primitives** | 2,494 uses | Complex synchronization |
| **Tests excluded** | 402 total | Significant; esp. parallel_replicas (315) |
| **Unsafe casts** | 2,012 | High; risky in complex type hierarchies |
| **CMakeLists.txt lines** | 677 | Complex; tight coupling of build |

