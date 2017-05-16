#pragma once

#include <DataTypes/IDataTypeDummy.h>


namespace DB
{

/** The data type corresponding to the set of values in the IN section.
  * Used only as an intermediate option when evaluating expressions.
  */
class DataTypeSet final : public IDataTypeDummy
{
public:
    std::string getName() const override { return "Set"; }
    DataTypePtr clone() const override { return std::make_shared<DataTypeSet>(); }
};

}

