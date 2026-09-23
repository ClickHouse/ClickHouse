#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_COLUMNS;
}

namespace
{

enum class GroupingVariant : uint8_t
{
    Ordinary,
    Rollup,
    Cube,
    GroupingSets,
};

/// Resolves the `grouping` function specializations from their trailing constant arguments,
/// which hold: the positions of the `grouping` arguments among the aggregation keys; for
/// `ROLLUP` and `CUBE` the total aggregation key count, or for `GROUPING SETS` the key positions
/// of every grouping set; and the `force_grouping_standard_compatibility` flag. The analyzer
/// appends them when it resolves `grouping` (see `GroupingFunctionsResolvePass`), so a serialized
/// query plan can rebuild the function from its name and arguments alone. The old analyzer
/// constructs the functions directly, with the state inside the function object and without the
/// trailing arguments; such plans cannot be serialized, which is fine there.
///
/// The trailing constants are read only once, here at build time. At execution they still arrive
/// with every block, but the function ignores them and computes from the state captured at
/// construction.
class GroupingSpecializationResolver : public IFunctionOverloadResolver
{
public:
    GroupingSpecializationResolver(String name_, GroupingVariant variant_)
        : name(std::move(name_)), variant(variant_)
    {
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Same as in `FunctionGroupingBase`: the keys among the arguments may be Nullable under
    /// `group_by_use_nulls`, and the result must stay plain UInt64.
    bool useDefaultImplementationForNulls() const override { return false; }

    /// Same as in `FunctionGroupingBase`: without this a single `LowCardinality` key among the
    /// arguments would wrap the result into `LowCardinality(UInt64)`.
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt64>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        const size_t num_trailing = variant == GroupingVariant::Ordinary ? 2 : 3;
        const size_t num_leading = variant == GroupingVariant::Ordinary ? 0 : 1;
        if (arguments.size() < num_leading + 1 + num_trailing)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least {} arguments", name, num_leading + 1 + num_trailing);

        /// The analyzer always satisfies the checks below, but the function can also be called
        /// directly with arbitrary arguments; reject those that would break the execution.
        if (variant != GroupingVariant::Ordinary && !WhichDataType(arguments[0].type).isUInt64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} must be the UInt64 grouping set index, got {}",
                name, arguments[0].type->getName());

        const size_t tail = arguments.size() - num_trailing;
        const auto arguments_indexes = getIndexes(arguments[tail]);
        const bool force_compatibility = getConstant(arguments.back()).safeGet<UInt64>() != 0;

        /// One index per key argument; without the check the result would describe a different
        /// argument list than the one the query spells.
        const size_t num_keys = arguments.size() - num_leading - num_trailing;
        if (arguments_indexes.size() != num_keys)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} has {} key arguments but {} argument indexes", name, num_keys, arguments_indexes.size());

        /// `FunctionGroupingBase::executeImpl` builds the result one bit per argument; without
        /// this check a wider mask would silently drop the high bits.
        if (variant != GroupingVariant::Ordinary && arguments_indexes.size() > 8 * sizeof(UInt64))
            throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
                "Too many arguments ({}) for function {}, the maximum is {}",
                arguments_indexes.size(), name, 8 * sizeof(UInt64));

        std::shared_ptr<IFunction> function;
        switch (variant)
        {
            case GroupingVariant::Ordinary:
            {
                /// `FunctionGroupingOrdinary` computes `1 << size` in the incompatible mode.
                if (!force_compatibility && arguments_indexes.size() >= 8 * sizeof(UInt64))
                    throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
                        "Too many arguments ({}) for function {}, the maximum is {}",
                        arguments_indexes.size(), name, 8 * sizeof(UInt64) - 1);
                function = std::make_shared<FunctionGroupingOrdinary>(arguments_indexes, force_compatibility);
                break;
            }
            case GroupingVariant::Rollup:
            {
                const auto keys_count = getConstant(arguments[tail + 1]).safeGet<UInt64>();
                validateIndexes(arguments_indexes, keys_count);
                function = std::make_shared<FunctionGroupingForRollup>(arguments_indexes, keys_count, force_compatibility);
                break;
            }
            case GroupingVariant::Cube:
            {
                const auto keys_count = getConstant(arguments[tail + 1]).safeGet<UInt64>();
                /// The same limit `CubeTransform` enforces at execution.
                if (keys_count >= 8 * sizeof(UInt64))
                    throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
                        "Too many keys ({}) are used for CUBE, the maximum is {}.", keys_count, 8 * sizeof(UInt64) - 1);
                validateIndexes(arguments_indexes, keys_count);
                function = std::make_shared<FunctionGroupingForCube>(arguments_indexes, keys_count, force_compatibility);
                break;
            }
            case GroupingVariant::GroupingSets:
            {
                ColumnNumbersList grouping_sets;
                for (const auto & set : getConstant(arguments[tail + 1]).safeGet<Array>())
                {
                    auto & indexes = grouping_sets.emplace_back();
                    for (const auto & index : set.safeGet<Array>())
                        indexes.push_back(index.safeGet<UInt64>());
                }
                function = std::make_shared<FunctionGroupingForGroupingSets>(arguments_indexes, grouping_sets, force_compatibility);
                break;
            }
        }

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.push_back(argument.type);

        return std::make_unique<FunctionToFunctionBaseAdaptor>(std::move(function), std::move(argument_types), result_type);
    }

private:
    Field getConstant(const ColumnWithTypeAndName & argument) const
    {
        if (!argument.column || !isColumnConst(*argument.column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The trailing arguments of function {} must be constants, got {}", name, argument.dumpStructure());
        return (*argument.column)[0];
    }

    ColumnNumbers getIndexes(const ColumnWithTypeAndName & argument) const
    {
        ColumnNumbers indexes;
        for (const auto & index : getConstant(argument).safeGet<Array>())
            indexes.push_back(index.safeGet<UInt64>());
        return indexes;
    }

    /// Without this an index at or above the key count would shift a UInt64 out of range in
    /// `FunctionGroupingForCube`.
    void validateIndexes(const ColumnNumbers & arguments_indexes, UInt64 keys_count) const
    {
        for (const auto index : arguments_indexes)
            if (index >= keys_count)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Function {}: argument index {} is out of range, there are {} aggregation keys",
                    name, index, keys_count);
    }

    const String name;
    const GroupingVariant variant;
};

}

REGISTER_FUNCTION(GroupingSpecializations)
{
    for (const auto & [name, variant] : std::initializer_list<std::pair<const char *, GroupingVariant>>{
            {"__groupingOrdinary", GroupingVariant::Ordinary},
            {"__groupingForRollup", GroupingVariant::Rollup},
            {"__groupingForCube", GroupingVariant::Cube},
            {"__groupingForGroupingSets", GroupingVariant::GroupingSets}})
    {
        factory.registerFunction(
            name,
            [function_name = String(name), function_variant = variant](ContextPtr) -> FunctionOverloadResolverPtr
            { return std::make_shared<GroupingSpecializationResolver>(function_name, function_variant); },
            FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
    }
}

}
