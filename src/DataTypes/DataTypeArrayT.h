#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

class DataTypeArrayT final : public DataTypeTuple
{
private:
    DataTypes elems;
    Strings names;
    bool have_explicit_names;

public:
    // TODO overwrite constructors
    // TODO overwrite getDefault
    // TODO remove for each child
    // TODO where is deserialisation
    // TODO remove all the logic related to names

    explicit DataTypeArrayT(const DataTypes & elems);
    DataTypeArrayT(const DataTypes & elems, const Strings & names);

    TypeIndex getTypeId() const override { return TypeIndex::Tuple; }
    std::string doGetName() const override;
    std::string doGetPrettyName(size_t indent) const override;
    const char * getFamilyName() const override { return "ArrayT"; }
    DataTypePtr getNormalizedType() const override;


    MutableColumnPtr createColumn() const override;
    MutableColumnPtr createColumn(const ISerialization & serialization) const override;

    void insertDefaultInto(IColumn & column) const override;
    // TODO rewrite
    // const DataTypePtr & getElement(size_t i) const { return elems[i]; }
    // const DataTypes & getElements() const { return elems; }
    // const Strings & getElementNames() const { return names; }
    // String getNameByPosition(size_t i) const;
};

}

