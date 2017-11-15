#pragma once

#include <DataTypes/IDataTypeDummy.h>


namespace DB
{

/** Special data type, representing lambda expression.
  */
class DataTypeExpression final : public IDataTypeDummy
{
private:
    DataTypes argument_types;
    DataTypePtr return_type;

public:
    static constexpr bool is_parametric = true;

    /// Some types could be still unknown.
    DataTypeExpression(const DataTypes & argument_types_ = DataTypes(), const DataTypePtr & return_type_ = nullptr)
        : argument_types(argument_types_), return_type(return_type_) {}

    std::string getName() const override;
    const char * getFamilyName() const override { return "Expression"; }

    DataTypePtr clone() const override
    {
        return std::make_shared<DataTypeExpression>(argument_types, return_type);
    }

    const DataTypes & getArgumentTypes() const
    {
        return argument_types;
    }

    const DataTypePtr & getReturnType() const
    {
        return return_type;
    }
};

}
