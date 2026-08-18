#include <Functions/FunctionPlannerOnlyFilter.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{

namespace
{

/// Planner-internal marker wrapping a predicate that was derived by the optimizer.
/// The wrapped predicate must be redundant (implied by the plan) so that both executing it and ignoring it at runtime are correct.
///
/// Not registered in `FunctionFactory`.
///
/// Currently used for IS NOT NULL predicates derived from joins in `deriveNotNullFiltersFromJoin.cpp`.
class FunctionPlannerOnlyFilter final : public IFunction
{
public:
    String getName() const override { return PLANNER_ONLY_FILTER_NAME; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    /// The wrapper must survive folding until it is consumed by the planner.
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return arguments.front(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        return arguments.front().column->convertToFullColumnIfConst();
    }
};

}

FunctionOverloadResolverPtr createInternalFunctionPlannerOnlyFilterResolver()
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionPlannerOnlyFilter>());
}

bool isPlannerOnlyFilterFunction(const IFunctionBase & function)
{
    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(&function);
    return adaptor && typeid_cast<const FunctionPlannerOnlyFilter *>(adaptor->getFunction().get());
}

}
