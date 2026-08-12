#pragma once

#include <DataTypes/DataTypeCustom.h>
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
    const DataTypes & getElements() const { return elems; }
    const Names & getNames() const { return names; }

    /// The Nested name embeds its element types, so rebuild it from the transformed elements. Built
    /// from the types directly, not the printed name: version 0 is deliberately not printed, so a
    /// name round trip would lose a leaf pinned to version 0.
    DataTypeCustomNamePtr transformChildren(const ChildTransform & transform) const override;
};

DataTypePtr createNested(const DataTypes & types, const Names & names);

template <typename DataType>
inline bool isNested(const DataType & data_type)
{
    return typeid_cast<const DataTypeNestedCustomName *>(data_type->getCustomName()) != nullptr;
}

}

