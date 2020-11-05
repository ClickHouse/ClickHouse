#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>
#include <DataTypes/DataTypeCustom.h>


namespace DB
{

class DataTypeNestedCustomName final : public IDataTypeCustomName
{
private:
    DataTypes elems;
    Strings names;

public:
    DataTypeNestedCustomName(const DataTypes & elems_, const Strings & names_)
        : elems(elems_), names(names_)
    {
    }

    String getName() const override;
};

template <typename DataType>
inline bool isNested(const DataType & data_type)
{
    return isArray(data_type) && typeid_cast<const DataTypeNestedCustomName *>(data_type->getCustomName());
}

}

