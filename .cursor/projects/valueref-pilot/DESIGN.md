# Design — `ValueRef`

Status: **proposal / not yet implemented**. Revise this file as the design evolves and note
changes in `PROGRESS_LOG.md`.

## Goal

Provide a lightweight, non-owning reference to a single value that already lives inside an
`IColumn`, so hot code can compare/inspect/copy a value without materializing a `Field`. This
directly attacks both problems from `INVESTIGATION.md`:

- No `Field` allocation/branching on the hot path (Problem 2).
- The reference is `(column, row)`, and the column knows its exact `IDataType`, so type identity is
  preserved (Problem 1) — no lossy `NearestFieldType` collapse.

## Core shape

```cpp
/// A non-owning reference to one value stored at row `row` of `column`.
/// Cheap to copy (a pointer + an index). Valid only while `column` outlives it and is not mutated
/// in a way that invalidates row `row`.
struct ValueRef
{
    const IColumn * column = nullptr;
    size_t row = 0;

    ValueRef() = default;
    ValueRef(const IColumn & column_, size_t row_) : column(&column_), row(row_) {}

    bool isValid() const { return column != nullptr; }

    /// Materialize into a Field only at the boundary where a Field is unavoidable.
    Field toField() const { return (*column)[row]; }
    void toField(Field & out) const { column->get(row, out); }
};
```

Design intent:
- **Trivially copyable, 16 bytes.** Never owns memory. Never heap-allocates.
- **Type-faithful:** `column->getDataType()` / an accompanying `DataTypePtr` gives the true SQL
  type; there is no `UInt8→UInt64` / `Float32→Float64` collapse.
- **`toField()` is the escape hatch,** used only at boundaries that genuinely need an owned value
  (AST literals, serialization, settings). The whole point is to *defer or avoid* that call.

## Operations the pilot needs

The value of `ValueRef` comes from operations that avoid `Field`. Candidate primitives (add
incrementally, only what a migrated call site needs):

- **Comparison against a column row:** reuse `IColumn::compareAt(row, other_row, other_column,
  nan_direction_hint)`. A `ValueRef`-to-`ValueRef` compare is
  `a.column->compareAt(a.row, b.row, *b.column, nan_hint)` when the columns are structurally equal.
- **Comparison against a `Field`/`ValueRef` for range checks** (for the `KeyCondition` target):
  needs a `compareAt`-style path that can take the "query constant" side. Investigate whether the
  constant can be held as a size-1 column (it already is, as `ColumnConst`) so both sides use
  `compareAt` and no `Field` appears.
- **Copy into another column:** `dst.insertFrom(*src.column, src.row)` (already exists, `Field`-free).
- **Null / default checks:** `column->isNullAt(row)`, `column->isDefaultAt(row)`.
- **Hashing:** `column->updateHashWithValue(row, hash)`.

Observation: most of these primitives **already exist on `IColumn`**. `ValueRef` is largely a thin,
ergonomic wrapper that makes "operate on a value in place" the easy, default choice instead of
`operator[]` → `Field`.

## Where it plugs in (pilot candidates, pick ONE first)

1. **`KeyCondition` / `Range` min-max checking (recommended first pilot).**
   - `INVESTIGATION.md` §D: the code already complains about `Field`/`FieldRef`/`Range` cost for
     long PKs (`MergeTreeDataSelectExecutor.cpp:1896-1963`).
   - Approach: let `FieldRef` hold an optional `ValueRef` and route `Range` boundary comparisons
     through column `compareAt` when both sides are column-backed, materializing a `Field` only for
     explicit query constants and `±∞` sentinels.
   - Risk: `Range` algebra + monotonic-function caching is intricate; must not change results.

2. **`SingleValueDataGeneric` aggregate state.**
   - Today stores a `Field value` and does `column.get(row, value)` (`SingleValueData.cpp`).
   - `SingleValueDataGenericWithColumn` and `SingleValueReference` already show the column-based
     alternative; the pilot could widen their use behind `canUseFieldForValueData`.
   - Lower blast radius than `KeyCondition`, self-contained, easy to benchmark with `min`/`max`/
     `any` over a generic type.

The recommended sequencing is in `ROADMAP.md`.

## Invariants and hazards

- **Lifetime/validity:** a `ValueRef` is only valid while its column is alive and row `row` is not
  invalidated (e.g. by `insert`, `popBack`, mutation). Document this at the type and never store a
  `ValueRef` past the lifetime of its column. This is the main correctness risk versus owning
  `Field`.
- **Structural equality for `compareAt`:** columns must be structurally comparable
  (`structureEquals`) — mirror the debug assertions `IColumn` already uses.
- **Const/Sparse/LowCardinality/Replicated:** `compareAt` and friends already handle these via
  their overrides; confirm the wrapper does not bypass them.
- **Nullability & NaN:** preserve `nan_direction_hint` semantics exactly.

## Explicitly NOT part of this design

- Removing `Field`, changing `ASTLiteral`, the settings API, or any on-disk/wire format
  (`INVESTIGATION.md` §B, §C).
- Changing the meaning of `IColumn::operator[]`/`get`/`insert` — we *add* alongside them.

## Success criteria

A `ValueRef` is worth keeping if, on the chosen pilot call site, it:
1. Removes per-value `Field` construction (verified by reading code + alloc/assembly inspection).
2. Is behavior-preserving (existing tests + a targeted `gtest_value_ref.cpp` pass).
3. Shows a measurable perf win (or at minimum no regression) on a representative benchmark, with
   numbers recorded in `PROGRESS_LOG.md`.
