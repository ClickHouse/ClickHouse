#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace
{

/// Planner-internal marker wrapping a predicate that was derived by the optimizer.
/// The wrapped predicate must be redundant (implied by the plan) so that both executing it and ignoring it at runtime are correct.
///
/// Currently used for IS NOT NULL predicates derived from joins in `deriveNotNullFiltersFromJoin.cpp`.
class FunctionPlannerOnlyFilter final : public IFunction
{
public:
    static constexpr auto name = "__plannerOnlyFilter";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPlannerOnlyFilter>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    /// The wrapper must survive folding until it is consumed my the planner.
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return arguments.front(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        return arguments.front().column;
    }
};

}

REGISTER_FUNCTION(PlannerOnlyFilter)
{
    factory.registerFunction<FunctionPlannerOnlyFilter>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS, FunctionFactory::Case::Sensitive);
}

}
