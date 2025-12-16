#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <pcg_random.hpp>

namespace DB
{

class FunctionGenerateRandomStructure : public IFunction
{
public:
    static constexpr auto name = "generateRandomStructure";

    explicit FunctionGenerateRandomStructure(bool allow_suspicious_lc_types_) : allow_suspicious_lc_types(allow_suspicious_lc_types_)
    {
    }

    static FunctionPtr create(ContextPtr context);

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    static String generateRandomStructure(size_t seed, const ContextPtr & context);

private:
    bool allow_suspicious_lc_types;
};

}
