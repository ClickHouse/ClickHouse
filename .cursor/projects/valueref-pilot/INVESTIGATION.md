# Investigation — the case for reducing `Field`

This file preserves the findings from the initial codebase survey (the "why"). All file:line
references were accurate as of the commit where this document was first added; verify against the
current tree if something looks off.

## Background: what `Field` is

`DB::Field` (`src/Core/Field.h`) is a hand-rolled tagged union (~40 bytes with libc++, heap
storage for `String`/`Array`/`Tuple`/`Map`/`Object`) representing one database value out of ~22
type tags (`Field::Types::Which`, `src/Core/Field.h:271-305`). `Row = std::vector<Field>`
(`src/Core/Field.h:669`).

Its own header advises against per-value use:

```260:267:src/Core/Field.h
/** Discriminated union of several types.
  * ...
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See IColumn.h
  */
```

## Problem 1 — `Field` type tags do NOT correspond to ClickHouse SQL types

The mapping is defined by `NearestFieldTypeImpl` (`src/Core/Field.h:167-238`) and is lossy and
many-to-one:

- `UInt8`, `UInt16`, `UInt32`, `Date`/`DayNum`, `DateTime`, `bool` → stored as **`UInt64`**
  (only `bool` gets a distinct `Bool` tag; the rest are indistinguishable).
- `Int8`, `Int16`, `Int32` → **`Int64`**.
- `Float32` → **`Float64`** — the header explicitly warns Float32 must not round-trip through
  `Field` (`src/Core/Field.h:279-282`).
- Enums → underlying integer type.
- `DateTime64`/`Time64` → `DecimalField`, blurring them with plain decimals.

Consequence: a `Field` alone cannot reconstruct the SQL type. Every consumer must carry a
`DataTypePtr` alongside, and there is a coercion ecosystem (`convertFieldToType`) plus special
bridges (`getFieldFromColumnForASTLiteral` in `src/Analyzer/Utils.cpp` for `DateTime64`,
`Dynamic`, `Object`) to compensate.

## Problem 2 — `Field` is mostly a value carrier, and an expensive one

`IColumn` bakes `Field` into its interface but flags it as slow:

```179:184:src/Columns/IColumn.h
    /// Returns value of n-th element in universal Field representation.
    /// Is used in rare cases, since creation of Field instance is expensive usually.
    [[nodiscard]] virtual Field operator[](size_t n) const = 0;
    /// Like the previous one, but avoids extra copying if Field is in a container, for example.
    virtual void get(size_t n, Field & res) const = 0;
```

`ColumnArray::get` even hard-caps materialization at `max_array_size_as_field = 1_000_000`
(`src/Columns/ColumnArray.cpp:40, 132-147`), and `ColumnTuple::get` builds a `Tuple` by calling
`operator[]` per element.

## Key finding — the "referral type" already exists in embryonic form: `FieldRef`

```14:37:src/Core/Range.h
/** A field, that can be stored in two representations:
  * - A standalone field.
  * - A field with reference to its position in a block.
  ...
struct FieldRef : public Field
{
    ...
    ColumnsWithTypeAndName * columns = nullptr;
    size_t row_idx = 0;
    size_t column_idx = 0;
};
```

But `FieldRef` **inherits from `Field` and copies the value into the `Field` base at
construction** (`src/Core/Range.cpp:13-16`), so it is a hybrid that still pays the `Field` cost.
It proves the concept but does not deliver the benefit. A clean `ValueRef` (column pointer + row
index, no owned value, no `Field` base) is the intended evolution — see `DESIGN.md`.

A second, aggregate-side precedent is `SingleValueReference` (column ref + row number, no `Field`)
in `src/AggregateFunctions/SingleValueData.*`.

## Map of where `Field` lives (roles, not just counts)

Rough scope: ~4,100 occurrences of the `Field` token across ~880 files under `src/`. They cluster
into four roles:

### A. Already column-backed / `Field`-free on hot paths (good news — leave alone)
- **`ColumnConst`** stores a **1-row `IColumn`** (`WrappedPtr data` + count `s`), not a `Field`
  (`src/Columns/ColumnConst.h:13-22, 64-72`). `getField()`/`getValue<T>()` are the only bridges.
- **`ConstantNode`/`ConstantValue`** (Analyzer) store a `ColumnConst`; `Field` is ingress/egress
  only (`src/Analyzer/ConstantValue.h`, `src/Analyzer/ConstantNode.h:56-60`).
- **`ActionsDAG::Node`** stores `ColumnConstPtr`; equality prefers `IColumn::compareAt`
  (`src/Interpreters/ActionsDAG.cpp:918-931`).
- **`ExpressionActions`, `PreparedSets`, input/output formats, the Native protocol** are already
  column-native.
- **Aggregate hot paths** use `SingleValueDataFixed`, arena strings, `SingleValueReference`.

### B. Genuine standalone value carriers (no backing column — hard to remove; OUT OF SCOPE)
- **`ASTLiteral::value`** — first representation of a parsed SQL literal
  (`src/Parsers/ASTLiteral.h:24-29`).
- **`convertFieldToType`** — detached-value coercion for `IN`, casts, key analysis, `VALUES`
  (`src/Interpreters/convertFieldToType.*`).
- **`evaluateConstantExpression`** — public contract is `std::pair<Field, DataTypePtr>`
  (`src/Interpreters/evaluateConstantExpression.h:21`).
- **Settings** — internal storage is typed, but the get/set/constraint API is `Field`-centric
  (`src/Core/BaseSettings.h:193-199`); `SettingChange{Field value}` flows through ALTER/protocol.
- **`±∞` sentinels** (`NEGATIVE_INFINITY`/`POSITIVE_INFINITY`), `FieldFromAST`.

### C. On-disk / on-wire (hardest — backward compat; OUT OF SCOPE for the pilot)
- MergeTree `partition.dat`, `minmax_*.idx`, skip-index `.idx2` deserialize into `Field` via
  `ISerialization::serializeBinary(Field&)`.
- Partition-ID hashing via `LegacyFieldVisitorHash` (`src/Storages/MergeTree/MergeTreePartition.cpp`).
- Column statistics V2+ via `writeFieldBinary`/`readFieldBinary`.
- Aggregate-function parameter persistence via `writeFieldBinary`
  (`src/Interpreters/AggregateDescription.cpp`).
- `ISerialization::serializeBinary(const Field&, ...)` is a required virtual on every data type
  (`src/DataTypes/Serializations/ISerialization.h:579-590`).

### D. Index/range algebra (central, performance-sensitive — PRIME PILOT TARGET)
- **`KeyCondition`/`Range`/`Hyperrectangle`** built on `FieldRef`
  (`src/Core/Range.h`, `src/Storages/MergeTree/KeyCondition.*`).
- `MergeTreeDataSelectExecutor.cpp:1896-1963` explicitly documents `Field`/`FieldRef`/`Range` as a
  bottleneck for long primary keys and already works around it by choosing explicit vs referential
  `FieldRef` and building "sparse arrays" to avoid `Field` where possible.

## Feasibility verdict

- **Full elimination of `Field`: not feasible.** Categories B and C have no column to reference
  and/or are compatibility surfaces requiring multi-release format migrations.
- **Reduction on hot paths: feasible and aligned with the code's own direction.** Highest
  value-to-risk targets:
  1. Introduce a first-class `ValueRef` (column ptr + row index, no `Field` base).
  2. Route index-analysis min/max checking (`KeyCondition`, category D) and/or generic aggregate
     state (`SingleValueDataGeneric`) through it.
  3. Shrink the `IColumn` `Field` surface by adding column-native comparison/extremes helpers and
     reducing `getValue<T>()`/`getField()` call sites.

The type-correspondence problem (Problem 1) is best addressed **not** by deleting `Field` but by
never passing a bare `Field` without its `DataTypePtr` — or by using size-1 `ColumnConst` + type,
which the `ConstantNode` model already does.

## Open questions to resolve during the pilot
- Can `FieldRef` be re-based on `ValueRef` (composition) without disturbing the `Range`/
  `KeyCondition` algebra and its monotonic-function caching?
- What is the measured cost of `Field` materialization in `KeyCondition::checkInHyperrectangle`
  for a long PK, and how much does `ValueRef` recover? (Needs a microbenchmark + a perf test.)
- Which `IColumn` comparison primitives are missing to let consumers avoid `Field` entirely
  (e.g. `compareAt` against a `ValueRef`)?
