#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** intersectAggregations(state1, state2, ...) - the state of the aggregate function over the intersection
  * of the sets of arguments aggregated in the given states, see `IAggregateFunction::intersect`.
  * Generalizes `bitmapAnd` and `uniqThetaIntersect` to every aggregate function that supports it.
  */
class FunctionIntersectAggregations final : public IFunction
{
public:
    static constexpr auto name = "intersectAggregations";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIntersectAggregations>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isVariadic() const override { return true; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    /// The arguments must be states with the same state representation, which the signature DSL cannot express,
    /// so the signature is documentation-only and `getReturnTypeImpl` below is authoritative.
    [[nodiscard]] String getSignatureString() const override { return "(A : AggregateFunction, AggregateFunction, ...) -> A"; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        types.reserve(arguments.size());
        for (const auto & argument : arguments)
            types.push_back(argument.type);
        return getReturnTypeImpl(types);
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", getName());

        const auto * first_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!first_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The arguments of function {} must be aggregate function states, but the first argument has type {}",
                getName(), arguments[0]->getName());

        const auto & function = first_type->getFunction();
        if (!function->canIntersect())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} does not support the states of type {}: aggregate function {} does not support the intersection of states",
                getName(), arguments[0]->getName(), function->getName());

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * type = typeid_cast<const DataTypeAggregateFunction *>(arguments[i].get());
            if (!type || !function->haveSameStateRepresentation(*type->getFunction()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The arguments of function {} must be states of the same aggregate function, "
                    "but argument {} has type {}, which is not compatible with the type of the first argument {}",
                    getName(), i + 1, arguments[i]->getName(), arguments[0]->getName());
        }

        return arguments[0];
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// The function of the type, not of the column: a column may hold the states under another function
        /// with the same state representation (for example, the one of a combinator).
        const auto & function = assert_cast<const DataTypeAggregateFunction &>(*result_type).getFunction();

        const size_t num_arguments = arguments.size();
        VectorWithMemoryTracking<const ColumnAggregateFunction::Container *> containers(num_arguments);
        VectorWithMemoryTracking<UInt8> is_const(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
        {
            const IColumn * column = arguments[i].column.get();
            if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
            {
                is_const[i] = true;
                column = &column_const->getDataColumn();
            }
            containers[i] = &assert_cast<const ColumnAggregateFunction &>(*column).getData();
        }

        /// Created by the type to carry its state version.
        auto result_column = result_type->createColumn();
        auto & result = assert_cast<ColumnAggregateFunction &>(*result_column);
        result.reserve(input_rows_count);
        Arena & arena = result.createOrGetArena();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            result.insertFrom((*containers[0])[is_const[0] ? 0 : row]);
            AggregateDataPtr place = result.getData().back();

            for (size_t i = 1; i < num_arguments; ++i)
                function->intersect(place, (*containers[i])[is_const[i] ? 0 : row], &arena);
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(IntersectAggregations)
{
    FunctionDocumentation::Description description = R"(
Takes several states of the same aggregate function and returns the state that the function would have
if only the values present in all of them were aggregated, i.e. the intersection of the sets of aggregated values.

It generalizes [`bitmapAnd`](/reference/functions/regular-functions/bitmap-functions#bitmapAnd)
and [`uniqThetaIntersect`](/reference/functions/regular-functions/uniqtheta-functions#uniqThetaIntersect)
to every aggregate function that supports the intersection of states:
- [`uniqExact`](/reference/functions/aggregate-functions/reference/uniqexact), [`groupUniqArray`](/reference/functions/aggregate-functions/reference/groupuniqarray) (without the `max_size` parameter) and [`groupBitmap`](/reference/functions/aggregate-functions/reference/groupbitmap) keep the exact set of values, so the result is exact;
- [`uniq`](/reference/functions/aggregate-functions/reference/uniq) keeps a deterministic sample of hashes of the values: the result is the sample of the intersection, taken with the coarser sampling of the two states, so it can be less precise than a state aggregated from the intersection directly;
- [`uniqTheta`](/reference/functions/aggregate-functions/reference/uniqthetasketch) applies the intersection of Theta sketches.

The states can be produced with combinators that keep the state of the nested function, such as `-If` or `-Array`,
or with `Nullable` arguments. The states of [`uniqCombined`](/reference/functions/aggregate-functions/reference/uniqcombined)
and [`uniqHLL12`](/reference/functions/aggregate-functions/reference/uniqhll12) cannot be intersected:
a HyperLogLog sketch cannot tell which values it contains.
)";
    FunctionDocumentation::Syntax syntax = "intersectAggregations(state1[, state2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"state1[, state2, ...]", "States of the same aggregate function, or of functions with the same state representation, e.g. `uniqState` and `uniqIfState` over the same types.", {"AggregateFunction"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the state of the intersection, of the same type as the first argument.", {"AggregateFunction"}};
    FunctionDocumentation::Examples examples = {
    {
        "Exact intersection",
        R"(
SELECT finalizeAggregation(intersectAggregations(
    uniqExactStateIf(number, number % 2 = 0),
    uniqExactStateIf(number, number % 3 = 0),
    uniqExactStateIf(number, number % 5 = 0))) AS divisible_by_30
FROM numbers(1000);
        )",
        R"(
┌─divisible_by_30─┐
│              34 │
└─────────────────┘
        )"
    },
    {
        "Intersection of the sets of values",
        R"(
SELECT arraySort(finalizeAggregation(intersectAggregations(
    groupUniqArrayStateIf(number % 10, number < 50),
    groupUniqArrayStateIf(number % 10, number % 4 = 0)))) AS res
FROM numbers(100);
        )",
        R"(
┌─res─────────┐
│ [0,2,4,6,8] │
└─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIntersectAggregations>(documentation);
}

}
