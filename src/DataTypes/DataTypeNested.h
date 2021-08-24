#pragma once

#include <DataTypes/IDataType.h>


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

DataTypePtr createNested(const DataTypes & types, const Names & names);

template <typename DataType>
inline bool isNested(const DataType & data_type)
{
    return typeid_cast<const DataTypeNestedCustomName *>(data_type->getCustomName()) != nullptr;
}

}

