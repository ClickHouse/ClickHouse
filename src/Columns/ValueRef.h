#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>

#include <cstddef>
#include <type_traits>


namespace DB
{

/** A non-owning reference to a single value stored at row `row` of `column`.
  *
  * The point of `ValueRef` is to let hot code inspect, compare, copy, or hash one value that
  * already lives inside an `IColumn` without materializing a `Field`. A `Field` allocates and
  * branches on its type tag; a `ValueRef` is just a pointer plus an index, so it is trivially
  * copyable and never touches the heap. It also preserves the exact type of the value, because the
  * referenced column knows its own `IDataType` — there is no lossy `NearestFieldType` collapse
  * (e.g. `UInt8` -> `UInt64`, `Float32` -> `Float64`) that a round trip through `Field` performs.
  *
  * Lifetime / validity: a `ValueRef` is valid only while `column` is alive and row `row` is not
  * invalidated (by `insert`, `popBack`, mutation, etc.). Never store a `ValueRef` past the lifetime
  * of the column it points into. This non-ownership is the whole benefit and the main hazard versus
  * an owning `Field`.
  *
  * `toField` is an escape hatch, meant for the boundaries where an owned value is genuinely
  * required (AST literals, serialization, settings). The goal is to defer or avoid that call, not
  * to make it convenient.
  */
struct ValueRef
{
    const IColumn * column = nullptr;
    size_t row = 0;

    ValueRef() = default;

    ValueRef(const IColumn & column_, size_t row_)
        : column(&column_), row(row_)
    {
    }

    /// A default-constructed `ValueRef` references nothing.
    bool isValid() const { return column != nullptr; }

    /// Materialize into an owned `Field`. Use only at a boundary that truly needs ownership.
    Field toField() const { return (*column)[row]; }

    /// Same as `toField`, reusing the caller's `Field` storage.
    void toField(Field & out) const { column->get(row, out); }

    /// Whether the referenced value is SQL `NULL`.
    bool isNull() const { return column->isNullAt(row); }

    /// Whether the referenced value equals the column's default (zero / empty).
    bool isDefault() const { return column->isDefaultAt(row); }

    /// Mix the referenced value into `hash`.
    void updateHashWithValue(SipHash & hash) const { column->updateHashWithValue(row, hash); }

    /// Append the referenced value to `dst`. Requires `dst` to be structurally compatible with the
    /// referenced column (the same precondition as `IColumn::insertFrom`).
    void insertInto(IColumn & dst) const { dst.insertFrom(*column, row); }

    /** Three-way comparison against another `ValueRef`, routed through `IColumn::compareAt` so no
      * `Field` is constructed. Returns a negative / zero / positive value when `*this` is less than
      * / equal to / greater than `rhs`.
      *
      * Precondition: both columns are structurally comparable (see `IColumn::structureEquals`),
      * mirroring what `IColumn::compareAt` itself requires. `nan_direction_hint` follows the exact
      * `IColumn::compareAt` semantics for `NULL` and `NaN` ordering.
      */
    int compareAt(const ValueRef & rhs, int nan_direction_hint) const
    {
        chassert(rhs.column != nullptr);
        chassert(column->structureEquals(*rhs.column));
        return column->compareAt(row, rhs.row, *rhs.column, nan_direction_hint);
    }
};

/// The whole point is that a `ValueRef` is as cheap to pass around as a raw pointer: no ownership,
/// no heap, memcpy-able. If this ever stops holding, the type has grown a responsibility it should
/// not have.
static_assert(std::is_trivially_copyable_v<ValueRef>);

}
