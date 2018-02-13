#pragma once

#include <DataTypes/IDataTypeDummy.h>


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
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }
    bool isParametric() const override { return true; }
};

}

