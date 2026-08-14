#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Planner-internal marker wrapping a predicate that was derived by the optimizer.
/// The wrapped predicate must be redundant (implied by the plan) so that both executing it and ignoring it at runtime are correct.
/// At plan time the marker is identity. At execution time the marker is replaced by constant-true.
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
    /// The wrapper must survive folding until it is consumed by the planner.
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & type = arguments.front();
        if (!isUInt8(removeNullable(removeLowCardinality(type))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} must be a predicate returning UInt8, while {} was given", getName(), type->getName());
        return type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        return arguments.front().column->convertToFullColumnIfConst();
    }
};

}

REGISTER_FUNCTION(PlannerOnlyFilter)
{
    factory.registerFunction<FunctionPlannerOnlyFilter>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS, FunctionFactory::Case::Sensitive);
}

}
