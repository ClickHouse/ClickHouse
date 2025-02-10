#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/** Immutable constant value representation during analysis stage.
  * Some query nodes can be represented by constant (scalar subqueries, functions with constant arguments).
  */
class ConstantValue;
using ConstantValuePtr = std::shared_ptr<ConstantValue>;

class ConstantValue
{
public:
    ConstantValue(Field value_, DataTypePtr data_type_)
        : value(std::move(value_))
        , data_type(std::move(data_type_))
    {}

    const Field & getValue() const
    {
        return value;
    }

    const DataTypePtr & getType() const
    {
        return data_type;
    }
private:
    Field value;
    DataTypePtr data_type;
};

}
