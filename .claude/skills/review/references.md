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

- `ITableFunction::getActualTableStructure` — reached by `DESCRIBE table_function(...)`
  (`InterpreterDescribeQuery`), `CREATE TABLE ... AS table_function(...)`
  (`InterpreterCreateQuery`), and `ITableFunction::execute` when cached columns disagree.
  `ITableFunction::getActualTableStructureWithAccess` does **not** cover this: it checks only
  *source* access (`READ ON MYSQL`, `READ ON S3`, …), derived from the storage engine name. A
  function that reads a ClickHouse table named in its arguments must check `SELECT` on that table
  itself, here — nothing in the framework does it.
- `ITableFunction::executeImpl` — constructing the storage.
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
of them. Grep the family, say which ones you checked, and report the siblings that share the
defect. As of this writing `mergeTreeIndex`, `mergeTreeTextIndex`, `mergeTreeProjection`, and the
`timeSeries*` functions all derive their structure from a source table fetched through
`DatabaseCatalog` in `getActualTableStructure` with no `SELECT` check there, while
`StorageMergeTreeIndex::read` and `StorageMergeTreeTextIndex::read` do check.

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
with no privilege on `db.t`, and its `BAD_ARGUMENTS` message disclosed the table's engine. Neither
path had a test. Fixed by https://github.com/ClickHouse/ClickHouse/pull/116647.

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
