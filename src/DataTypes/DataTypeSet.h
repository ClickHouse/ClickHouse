#pragma once

#include <DataTypes/IDataTypeDummy.h>
#include <Columns/ColumnSet.h>


namespace DB
{

/** The data type corresponding to the set of values in the IN section.
  * Used only as an intermediate when evaluating expressions.
  */
class DataTypeSet final : public IDataTypeDummy
{
public:
    static constexpr bool is_parametric = true;
    const char * getFamilyName() const override { return "Set"; }

    TypeIndex getTypeId() const override { return TypeIndex::Set; }
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }
    bool isParametric() const override { return true; }

    // Used for expressions analysis.
    MutableColumnPtr createColumn() const override { return ColumnSet::create(0, nullptr); }

    // Used only for debugging, making it DUMPABLE
    Field getDefault() const override { return Tuple(); }

    // For DataTypeSet, we rely on the base class implementation which includes the type ID
};

}

