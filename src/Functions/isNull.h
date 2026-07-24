#pragma once
#include <Functions/IFunction.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull : public IFunction
{
public:
    static constexpr auto name = "isNull";

    static FunctionPtr create(ContextPtr context);

    explicit FunctionIsNull(bool use_analyzer_) : use_analyzer(use_analyzer_) {}

    std::string getName() const override { return name; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;

private:
    bool use_analyzer;
};

}
