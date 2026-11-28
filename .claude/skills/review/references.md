# Review references

Detailed procedures for triggered expansions that fire on a small fraction of PRs.
Read the relevant section only when its trigger appears in the diff; the base checklist
in `SKILL.md` names the trigger and points here.

## Build flag that drops a global side-effect or weakens numeric semantics {#build-flags}

Trigger: the diff changes compiler/build flags that remove a documented side-effect or relax
FP behavior across many translation units — `-fno-math-errno` (libm stops setting `errno`),
`-ffast-math` / `-ffp-contract=fast` / `-fassociative-math` / `-freciprocal-math` /
`-fno-signed-zeros` (reordered/contracted/round-changed FP → non-reproducible results),
`-fno-trapping-math`, strict-aliasing relaxation, and similar.

The PR's implicit promise is "no code relying on the old behavior is affected"; treat that as
the contract and verify it. A grep over the affected symbol is not proof, because a match only
matters if it is **(a) compiled with the flag** and **(b) actually depends on the specific
behavior the flag removes**, not merely mentions the symbol.

- **(a) build graph (general):** a match in code not compiled with the flag cannot be affected —
  `rust_vendor`/cargo crates, `EXCLUDE_FROM_ALL`, disabled `ENABLE_*` options, or an uncompiled
  part of a contrib (e.g. a vendored DB's server backend when only its client library is built).
- **(b) what a true consumer looks like is flag-specific** — work it out per flag, don't reuse
  one flag's shape for another. For `-fno-math-errno` the affected behavior is `errno` after a
  `<math.h>` call, so most matches are false: code that *sets* errno (a libm's own
  `errno = EDOM` / `with_errno(...)`, an error-code table, `return ERANGE`) is not a consumer —
  only a *read* after a math call is; and a read whose producer the flag does not touch
  (`strtod`/`strtol`/`scanf`, a syscall) is unaffected. For reproducibility-affecting FP flags
  (`-ffast-math`/contract/associative/signed-zeros) there is no "setter/reader" split at all —
  the consumer is anything that assumes bit-identical results: cross-platform/cross-ISA
  reproducibility, distributed-merge/aggregation consistency, and committed test references — so
  the cost is acknowledged and reasoned about, not grep-audited.

Also check **scope intent:** flags appended to `COMPILER_FLAGS` before `add_subdirectory(contrib …)`
propagate into vendored libraries. Contrib coverage may be deliberate (often it is where the
speedup lives) or unsafe, but it must be a conscious, stated decision with any exceptions carved
out per target. A real consumer that silently loses the behavior is a correctness/compatibility
**Blocker**; an unstated contrib-wide reproducibility change is at least a **Major**.

## Access and privilege checks {#access-checks}

Trigger: the diff adds, moves, removes, or relies on an access check (`context->checkAccess`,
`getAccess()->checkAccess*`, `checkAccessRights`, `AccessType::*`, row policies, readonly /
`allow_ddl` gates), **or** it adds a surface whose result is derived from an object the querying
user may not be allowed to read — a table function or engine over another table, a system table,
an introspection function, a new subcolumn, new `DESCRIBE` / `EXPLAIN` / `SHOW` output.

### Completeness before granularity

Answer "is the check reached on every path, before anything is derived?" **before** "is the check
the right shape?". Critiquing an `AccessType`, a column granularity, or a strictness choice reads
like a security finding and is easy to reach, but it leaves the prior question open, and a path
that skips the check entirely is the worse defect. An author's "the strict check is intentional"
answers the shape question only.

### Entrypoint inventory

A new surface has more entrypoints than the one the check was written for. For a table function
that reads another ClickHouse object:

- `ITableFunction::parseArguments` — **inspect this first**: `TableFunctionFactory::get` calls it
  before *any* access check, so it is the earliest carrier and the easiest one to overlook, being
  named as if it only parsed syntax. Argument parsing routinely does not stop at literals: it
  resolves the storage ID (`Context::resolveStorageID`), fetches the table
  (`DatabaseCatalog::instance().getTable`), casts it to an expected engine, and reads its metadata
  to store a type name or a column type on the table-function object. Follow every helper it
  delegates to — a storage's `getConfiguration` is a common one — and note that a `typeid_cast`
  helper throws too: `storagePtrToTimeSeries` reports that the engine of the named table is not
  `TimeSeries`, which discloses the table's existence and one negative fact about its engine without
  naming the engine, where the `MergeTree` helpers print `got: {}` and name it outright. Both are
  disclosures; grade them, but do not treat the weaker one as harmless. Anything resolved here leaks
  before the check that a later entrypoint performs, so a check added only to
  `getActualTableStructure` or `read` does not cover it.
- `ITableFunction::getActualTableStructure` — reached by `DESCRIBE table_function(...)`
  (`InterpreterDescribeQuery`), `CREATE TABLE ... AS table_function(...)`
  (`InterpreterCreateQuery`), and `ITableFunction::execute`, which calls it to validate non-empty
  cached columns against the actual structure:
  `hasStaticStructure() && cached_columns == getActualTableStructure(...)`. The call is the
  comparison operand, so for a static-structure function it runs on **every** `execute` that has
  cached columns, whichever way the comparison then comes out — do not read it as an exceptional
  path taken only on a mismatch. When the structure is not static, `execute` does not call it at
  all: it returns a `StorageTableFunctionProxy` that resolves lazily through `executeImpl`.
  `ITableFunction::getActualTableStructureWithAccess` does **not** cover this: it checks only
  *source* access (`READ ON MYSQL`, `READ ON S3`, …), derived from the storage engine name. A
  function that reads a ClickHouse table named in its arguments must check `SELECT` on that table
  itself, here — nothing in the framework does it.
- `ITableFunction::executeImpl` — constructing the storage. A function whose declared structure is a
  fixed column list still resolves and validates the source table here, so do not conclude from a
  static `getActualTableStructure` that nothing is derived before `read`. Descend into the storage
  **constructor** it calls: validation placed there (a `dynamic_cast` to `MergeTreeData` and its
  `BAD_ARGUMENTS`) runs while the surface is being built, ahead of any check in `read`.
- `IStorage::read` — the read itself.
- Anything that answers a question about the object without going through `read`: `totalRows`,
  `totalRowsByPartitionPredicate`, `totalBytes`, `totalBytesUncompressed`,
  `getQueryProcessingStage`, trivial-count optimization, capability probes.
- Mutating operations, if the surface has any: `INSERT INTO FUNCTION`, `TRUNCATE`, `ALTER`, `DROP`,
  `RENAME`. A class that adds a guard usually does not override the inherited ones.

For a system table the analogue is per-row filtering in `fillData` **and** `read`; for a subcolumn
or introspection function it is every path that can render the value.

### Metadata and error text are protected information

"No data leaked, the read is checked" is not a defence. Column names and types, the engine name,
part names, sizes, row counts, existence, and the error code itself are all information about the
object. So the check must run **before** anything is derived from it: before validation, before a
`dynamic_cast` diagnostic, before structure derivation, and before any exception whose message
names a property of the object. `expected MergeTree table, got: Log` thrown ahead of the privilege
check tells an unprivileged user the engine of a table it cannot select from. Distinguishing
`UNKNOWN_TABLE` from `ACCESS_DENIED` likewise discloses existence: follow what the surrounding
code already does, but never let a new message be more specific than the check that guards it.

### Tests: one negative case per entrypoint

Requiring a privilege is user-visible behavior, so each guarded path needs its own case: the read
without the grant, the structure resolution (`DESCRIBE`) without the grant, a partial grant that
must still be denied, and the full grant that must succeed. A check whose only test is the path it
was written for proves nothing about the others; a check with no test is a **Major** on its own.

### Sibling surfaces

Surfaces of one family are written from each other's template, so a defect in one is usually in all
of them. Enumerate the family from the **registrations** — `factory.registerFunction` and
`registerAlias` for table functions, the equivalent registry elsewhere — rather than from the files
or class names, since one class can be registered under several names with a constructor flag that
changes how it resolves its target. Then say which ones you checked, and report the siblings that
share the defect.
Do not assume the family shares one carrier: locate the earliest resolution per function, because
the entrypoint that leaks differs even between siblings, and a sweep that looks at
`getActualTableStructure` alone will clear the ones that leak somewhere else.

The illustration below is the `mergeTree*` and `timeSeries*` table functions as of this writing —
one family, swept deliberately, not an inventory of every table function in the server. Redo the
sweep rather than trusting this list; the point is the *shape*, which is that the check sits at the
read while every function resolves its source table before that, in one of three places:

- `getActualTableStructure` — `mergeTreeIndex`, `mergeTreeProjection`
  (`TableFunctionMergeTreeProjection`, in `TableFunctionProjection.cpp`), and
  `mergeTreeCodecBlockCounts`. Each throws before returning: `expected MergeTree table, got: {}`,
  `There is no projection {} in table {}`.
- `executeImpl` — `mergeTreeTextIndex`, and both `mergeTreeAnalyzeIndexes` and
  `mergeTreeAnalyzeIndexesUUID`, whose `getActualTableStructure` is a fixed column list that touches
  no table. `mergeTreeTextIndex` throws `Got index '{}' of type '{}', expected 'text'` there; for the
  analyze-indexes pair the disclosure is one level deeper still, in the
  `StorageMergeTreeAnalyzeIndexes` **constructor**
  (`Storage MergeTreeAnalyzeIndexes expected MergeTree table, got: {}`), which `executeImpl` calls.
  A storage constructor invoked while building the surface is an entrypoint too.

  The two analyze-indexes names are one class, `TableFunctionMergeTreeAnalyzeIndexes`, registered
  twice with a `resolve_by_uuid` constructor flag. That is worth its own note: **enumerate a family
  from the registrations, not from the files or the class names**, because one class can expose
  several functions and reading the file suggests one surface. The `UUID` variant also addresses its
  table by UUID through `DatabaseCatalog::tryGetByUUID`, so it never calls `resolveStorageID` and
  there is no database or table name in its arguments at all — a guard phrased as "check `SELECT` on
  the database and table the user named" has nothing to read, and must work from the resolved
  storage's own `StorageID` instead. Both are registered `allow_readonly`, so the
  `CREATE_TEMPORARY_TABLE` check in `execute` does not fire for them either.
- `parseArguments` — `timeSeriesSamples` (aliased `timeSeriesData`) / `timeSeriesMetrics` /
  `timeSeriesTags` (`TableFunctionTimeSeriesTarget`), which calls `getTargetTable` to store the
  target engine name, so the leak precedes `getActualTableStructure` even though that derives
  columns from the source too; `timeSeriesSelector`, via
  `StorageTimeSeriesSelector::getConfiguration`; and `prometheusQuery` / `prometheusQueryRange`, via
  `StoragePrometheusQuery::getConfiguration`. For all three of the latter,
  `getActualTableStructure` derives columns from the parsed configuration and touches no table.

  `prometheusQuery` is the reason the registration rule above matters. It is registered in
  `TableFunctionTimeSeries.cpp`, immediately beside the `timeSeries*` functions, from a class that
  resolves its source through the same `getConfiguration` pattern — but it does not share the name
  stem, so a family sweep driven by grepping `timeSeries` misses both variants entirely. Family
  membership is defined by the shape of the code, not by the prefix of the name.

The read side has three outcomes, not two, and the third is the one worth learning.

1. **Checks directly.** Every `mergeTree*` function: `read` in `StorageMergeTreeIndex`,
   `StorageMergeTreeTextIndex`, `StorageMergeTreeCodecBlockCounts` and
   `StorageFromMergeTreeProjection` (which also applies row policies), and `readImpl` in
   `StorageMergeTreeAnalyzeIndexes`. Each names the source table explicitly in its own
   `context->checkAccess(AccessType::SELECT, …)`.
2. **Inherits the check by delegating.** `timeSeriesSelector`, and equally `prometheusQuery` /
   `prometheusQueryRange`: `StorageTimeSeriesSelector::readImpl` and
   `StoragePrometheusQuery::readImpl` build inner `SELECT` ASTs whose `FROM` is an
   `ASTTableIdentifier` for the tags and samples tables and run them through
   `InterpreterSelectQueryAnalyzer`. Those are ordinary `TableNode`s, so they take the
   `if (table_node)` branch of `PlannerJoinTree`'s `checkAccessRights` and get a real column-aware
   `SELECT` check on the *target* tables — without either storage containing a `checkAccess` call of
   its own. Grep alone would score these surfaces as unguarded. What they still never check is
   `SELECT` on the *source* `TimeSeries` table named in the arguments.
3. **Loses the check by returning a real storage.** `timeSeriesSamples` (and its alias
   `timeSeriesData`) / `timeSeriesMetrics` / `timeSeriesTags`: `executeImpl` returns the target
   table's own storage rather than a wrapper, so
   there is no inner query to inherit a check from, and the outer node is a `TableFunctionNode` —
   which both planners deliberately exclude ("we do not check access rights for table functions
   because they have been already checked in `ITableFunction::execute`", `PlannerJoinTree.cpp`, and
   `!joined_tables.isLeftTableFunction()` in `InterpreterSelectQuery.cpp`). `execute` in turn checks
   only `getSourceAccessObject`, derived from the engine name, which for a `MergeTree` target is
   nothing. So neither the source nor the target is checked on any path.

Two lessons. First, "does this surface check?" is not a grep question: a storage with no
`checkAccess` may be fully guarded because it delegates to inner queries (2), and a storage that
returns someone else's real storage may be wholly unguarded precisely *because* the framework
believes a table function already checked (3). Second, the framework's exclusion of table-function
nodes means `ITableFunction::execute` is the only guard the planner assumes exists — so any table
function naming a ClickHouse table in its arguments must check `SELECT` itself, and case 3 shows what
happens when the surface is derived from a real table and nobody does.

### Severity

An entrypoint that reaches protected data **or metadata** without the check is a **Blocker**, even
when no row data leaks. A check that is merely too coarse — it denies a grant that ought to suffice
— is at most a **Major**, and is a legitimate design choice once documented and tested.

### Worked example

`mergeTreeCodecBlockCounts` (https://github.com/ClickHouse/ClickHouse/pull/109623) checked `SELECT`
on the source table in `StorageMergeTreeCodecBlockCounts::read` only. The review found that check
and questioned its column granularity; the author defended it as intentional, and the review
stopped there. Meanwhile `TableFunctionMergeTreeCodecBlockCounts::getActualTableStructure` went
straight to `DatabaseCatalog`, so `DESCRIBE mergeTreeCodecBlockCounts(db, t)` succeeded for a user
with no privilege on `db.t`, and its `BAD_ARGUMENTS` message disclosed the table's engine.

The test situation is worth stating precisely, because it is the evidence gap in its usual form. The
function was not short of tests: ten `.sql` files, and both code paths were exercised —
`04267_mergeTreeCodecBlockCounts_basic.sql` hits the non-`MergeTree` `BAD_ARGUMENTS` on the
structure path, and `04509_mergeTreeCodecBlockCounts_row_policy.sql` asserts `ACCESS_DENIED` at read
time. What none of them does is run as a user that lacks the grant: not one creates a user or grants
anything, and not one issues a `DESCRIBE`. So the `checkAccess(AccessType::SELECT, …)` in `read` was
itself untested — `04509` covers the row-policy branch beside it, which is a different guard — and
nothing in the suite distinguished a check on one entrypoint from a check on all of them. The gap was
never "no tests"; it was "no negative case", which is why the rule above asks for one per guarded
entrypoint rather than for coverage of the paths.

The fix is https://github.com/ClickHouse/ClickHouse/pull/116647, still open at the time of writing,
so the structure path is unguarded in `master` — which is why the function appears in the sibling
list above rather than as a closed case.

## Native protocol / native format spec sync {#spec-sync}

Trigger (protocol): the diff touches the native TCP protocol — `src/Core/Protocol.h`,
`src/Core/ProtocolDefines.h`, `src/Core/Protocol.cpp`, packet handling in `src/Server/TCPHandler.*`
or `src/Client/Connection.*`, the `DBMS_TCP_PROTOCOL_VERSION` / `DBMS_MIN_REVISION_*` constants,
packet types, handshake/version negotiation, or the wire layout of any non-`Block` message.

Trigger (format): the diff changes the `Native` format — its wire/serialization format, type
encodings (`LowCardinality`, `Array`, `Map`, `Variant`, `Dynamic`, `JSON`), the block/column
structure, the compression frame, `NativeReader`/`NativeWriter`, or `docs/reference/formats/Native.mdx`.

Verify the corresponding specification is updated in the **same PR**:
`docs/reference/interfaces/specs/NativeProtocol.mdx` for protocol changes,
`docs/reference/interfaces/specs/NativeFormat.mdx` for format changes. The spec is the canonical reference
third-party native clients (`ch-go`, `clickhouse-go`) are built against; letting it drift forces
re-deriving the protocol/format from C++ source. This applies to new features, bug fixes, and
behavior changes alike. Flag a missing or stale spec section as a **Major**, naming the
packet/version/field/encoding the diff changed.
