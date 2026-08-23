#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/** An owning single constant value: a size-1 (const) column plus its exact SQL type.
  *
  * This is the type-consistent, copy-free representation of a scalar constant used across the analyzer
  * and the constant-expression / coercion layer, replacing raw `{ColumnPtr, DataTypePtr}` pairs. Unlike
  * `Field`, it keeps the precise type (`UInt8`, `Float32`, `DateTime64`, ...) instead of collapsing it
  * through `NearestFieldType`, and reading a scalar via the typed getters below does not materialize a
  * `Field`.
  *
  * Lives in `Core` (not `Columns`) because it carries a `DataTypePtr`, and `DataTypes` depends on
  * `Columns`. It is intentionally distinct from `ColumnWithTypeAndName`: it guarantees a size-1
  * `ColumnConst`, has no name, and exposes value accessors.
  */
class ConstantValue
{
public:
    ConstantValue(ColumnConstPtr column_, DataTypePtr data_type_)
        : column(std::move(column_))
        , data_type(std::move(data_type_))
    {}

    /// Transitional `Field` entry point: builds the size-1 `ColumnConst` from a `Field`. Kept while the
    /// analyzer still produces `Field`s upstream; tracked as a bridge in the ValueRef pilot's BRIDGES.md.
    ConstantValue(const Field & field_, DataTypePtr data_type_)
        : column(data_type_->createColumnConst(1, field_))
        , data_type(std::move(data_type_))
    {}

    const ColumnConstPtr & getColumn() const
    {
        return column;
    }

    const DataTypePtr & getType() const
    {
        return data_type;
    }

    /// Value accessors - read row 0 of the size-1 column without materializing a `Field`.
    bool isNull() const { return column->isNullAt(0); }
    UInt64 getUInt() const { return column->getUInt(0); }
    Int64 getInt() const { return column->getInt(0); }
    Float64 getFloat64() const { return column->getFloat64(0); }
    bool getBool() const { return column->getBool(0); }
    std::string_view getDataAt() const { return column->getDataAt(0); }

    /// Transitional: materialize the value as a `Field`. Prefer the typed getters above; this exists for
    /// callers not yet migrated off `Field` (e.g. `ConstantNode::getValue`).
    Field getField() const
    {
        Field field;
        column->get(0, field);
        return field;
    }

    String getValueName(const IColumn::Options & options) const
    {
        return column->getValueName(0, options);
    }

    static ColumnConstPtr wrapToColumnConst(const ColumnPtr & column_)
    {
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column_.get()))
            return column_const->getPtr();
        return ColumnConst::create(column_, 1);
    }

private:
    ColumnConstPtr column;
    DataTypePtr data_type;
};

}
