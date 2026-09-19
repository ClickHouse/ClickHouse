#include "config.h"

#include <Storages/MergeTree/MergeTreePartInfo.h>
#if CLICKHOUSE_CLOUD
#include <Storages/SharedMergeTree/MergeMutateIntention.h>
#endif

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>

#include <Common/register_objects.h>

#include <algorithm>
#include <cctype>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct UnpackedPartSegments
{
    MergeTreePartInfo part_info;
    std::string prefix;
    std::string suffix;
};

std::optional<MergeTreePartInfo> tryParseMergeTreePartInfo(std::string_view data)
{
    return MergeTreePartInfo::tryParsePartName(data, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
}

[[noreturn]] void throwInvalidPartName(std::string_view data)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MergeTree part name: '{}'.", data);
}

MergeTreePartInfo constructPartInfo(std::string_view data)
{
    if (auto info = tryParseMergeTreePartInfo(data))
        return std::move(info.value());

    throwInvalidPartName(data);
}

bool isTryNSuffix(std::string_view suffix)
{
    return suffix.size() > 3
        && suffix.starts_with("try")
        && std::all_of(suffix.begin() + 3, suffix.end(), [](unsigned char c) { return std::isdigit(c); });
}

/// Tries to parse part name format: "<prefix>_<part_name>_<tryN>".
UnpackedPartSegments unpackPartName(std::string_view data, bool is_detached)
{
    UnpackedPartSegments unpacked;

    /// prefix_name_tryN
    ///                 ^
    size_t right = data.size();
    size_t first_delimiter = data.find_first_of('_');
    size_t last_delimiter = data.find_last_of('_');
    size_t number_of_segments = std::ranges::count(data, '_') + 1;

    if (number_of_segments < 4)
        throwInvalidPartName(data);

    /// Process suffix first because it can be easily determined.
    const auto suffix = data.substr(last_delimiter + 1);
    if (suffix.starts_with("try") && (!is_detached || isTryNSuffix(suffix)))
    {
        unpacked.suffix = suffix;
        right = last_delimiter;
        number_of_segments -= 1;
    }

    /// A known detach reason is authoritative in detached mode. Do not fall back to parsing the whole name as a regular part.
    if (is_detached)
    {
        for (std::string_view known_prefix : DetachedPartInfo::DETACH_REASONS)
        {
            if (data.starts_with(known_prefix)
                && known_prefix.size() < right
                && data[known_prefix.size()] == '_')
            {
                const auto part_name = data.substr(known_prefix.size() + 1, right - known_prefix.size() - 1);
                if (auto info = tryParseMergeTreePartInfo(part_name))
                {
                    unpacked.prefix = known_prefix;
                    unpacked.part_info = std::move(info.value());
                    return unpacked;
                }

                throwInvalidPartName(data);
            }
        }
    }

    switch (number_of_segments)
    {
    case 6: /// prefix_partition_min_max_level_mutation
    {
        unpacked.prefix = data.substr(0, first_delimiter);
        unpacked.part_info = constructPartInfo(data.substr(first_delimiter + 1, right - (first_delimiter + 1)));
        break;
    }
    case 5: /// prefix_partition_min_max_level or partition_min_max_level_mutation
    {
        if (auto info = tryParseMergeTreePartInfo(data.substr(0, right)))
        {
            unpacked.prefix = "";
            unpacked.part_info = std::move(info.value());
            break;
        }

        if (auto info = tryParseMergeTreePartInfo(data.substr(first_delimiter + 1, right - (first_delimiter + 1))))
        {
            unpacked.prefix = data.substr(0, first_delimiter);
            unpacked.part_info = std::move(info.value());
            break;
        }

        throwInvalidPartName(data);
    }
    case 4: /// partition_min_max_level
    {
        unpacked.part_info = constructPartInfo(data.substr(0, right));
        break;
    }
    default:
        throwInvalidPartName(data);
    }

    return unpacked;
}

bool isAnyStringType(const IDataType & data_type)
{
    return isStringOrFixedString(removeLowCardinality(data_type.getPtr()));
}

bool isBoolType(const IDataType & data_type)
{
    return data_type.getName() == "Bool";
}

class FunctionMergeTreePartCoverage final : public IFunction
{
    static MergeTreePartInfo constructCoveringPart(const ColumnPtr & covering_column, size_t row_number, bool is_detached)
    {
        if (isColumnConst(*covering_column))
            return unpackPartName(covering_column->getDataAt(0), is_detached).part_info;

        return unpackPartName(covering_column->getDataAt(row_number), is_detached).part_info;
    }

public:
    static constexpr auto name = "isMergeTreePartCoveredBy";
    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMergeTreePartCoverage>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"nested_part", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"},
            {"covering_part", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"is_detached",
             static_cast<FunctionArgumentDescriptor::TypeValidator>(&isBoolType), isColumnConst, "const Bool"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt8::create();
        const ColumnPtr & input_column = arguments.front().column;
        const bool is_detached = arguments.size() == 3 && arguments[2].column->getBool(0);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto part_info = unpackPartName(input_column->getDataAt(i), is_detached).part_info;
            const auto covering_part = constructCoveringPart(arguments[1].column, i, is_detached);
            result_column->insertValue(covering_part.contains(part_info));
        }

        return result_column;
    }
};

class FunctionMergeTreePartInfo final : public IFunction
{
public:
    static constexpr auto name = "mergeTreePartInfo";
    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMergeTreePartInfo>(); }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"part_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"is_detached", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isBoolType), isColumnConst, "const Bool"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        DataTypes types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
        };

        Names names = {
            "partition_id",
            "prefix",
            "suffix",
            "min_block",
            "max_block",
            "level",
            "mutation",
        };

        return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & input_column = arguments[0].column;
        const bool is_detached = arguments.size() == 2 && arguments[1].column->getBool(0);

        auto partition_column = ColumnString::create();
        auto prefix_column = ColumnString::create();
        auto suffix_column = ColumnString::create();
        auto min_block_column = ColumnInt64::create();
        auto max_block_column = ColumnInt64::create();
        auto level_column = ColumnInt64::create();
        auto mutation_column = ColumnInt64::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto [part_info, prefix, suffix] = unpackPartName(input_column->getDataAt(i), is_detached);

            partition_column->insertData(part_info.getPartitionId().data(), part_info.getPartitionId().size());
            prefix_column->insertData(prefix.data(), prefix.size());
            suffix_column->insertData(suffix.data(), suffix.size());
            min_block_column->insertValue(part_info.min_block);
            max_block_column->insertValue(part_info.max_block);
            level_column->insertValue(part_info.level);
            mutation_column->insertValue(part_info.mutation);
        }

        return ColumnTuple::create(Columns{
            std::move(partition_column),
            std::move(prefix_column),
            std::move(suffix_column),
            std::move(min_block_column),
            std::move(max_block_column),
            std::move(level_column),
            std::move(mutation_column),
        });
    }
};

}

REGISTER_FUNCTION(MergeTreePartInfoTools)
{
    FunctionDocumentation::Description description_coverage = R"(
Function which checks if the part of the first argument is covered by the part of the second argument.
    )";
    FunctionDocumentation::Syntax syntax_coverage = "isMergeTreePartCoveredBy(nested_part, covering_part[, is_detached])";
    FunctionDocumentation::Arguments arguments_coverage = {
        {"nested_part", "Name of expected nested part.", {"String"}},
        {"covering_part", "Name of expected covering part.", {"String"}},
        {"is_detached",
         "If true, parse both names as detached parts. Both arguments must use the same naming convention.",
         {"const Bool"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_coverage = {"Returns `1` if it covers, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_coverage = {
    {
        "Basic example",
        R"(
WITH 'all_12_25_7_4' AS lhs, 'all_7_100_10_20' AS rhs
SELECT isMergeTreePartCoveredBy(rhs, lhs), isMergeTreePartCoveredBy(lhs, rhs);
        )",
        R"(
┌─isMergeTreePartCoveredBy(rhs, lhs)─┬─isMergeTreePartCoveredBy(lhs, rhs)─┐
│                                  0 │                                  1 │
└────────────────────────────────────┴────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_coverage = {25, 6};
    FunctionDocumentation::Category category_coverage = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation_coverage = {description_coverage, syntax_coverage, arguments_coverage, {}, returned_value_coverage, examples_coverage, introduced_in_coverage, category_coverage};

    factory.registerFunction<FunctionMergeTreePartCoverage>(documentation_coverage, FunctionFactory::Case::Insensitive);

    FunctionDocumentation::Description description_info = R"(
Function that helps to cut the useful values out of the `MergeTree` part name.
    )";
    FunctionDocumentation::Syntax syntax_info = "mergeTreePartInfo(part_name[, is_detached])";
    FunctionDocumentation::Arguments arguments_info = {
        {"part_name", "Name of part to unpack.", {"String"}},
        {"is_detached", "If true, parse the name as a detached part. This is needed for names which are also valid regular part names.", {"const Bool"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_info = {
        "Returns a Tuple with subcolumns: `partition_id`, `prefix`, `suffix`, "
        "`min_block`, `max_block`, `level`, `mutation`.", {"Tuple"}};
    FunctionDocumentation::Examples examples_info = {
    {
        "Basic example",
        R"(
WITH mergeTreePartInfo('all_12_25_7_4') AS info
SELECT info.partition_id, info.min_block, info.max_block, info.level, info.mutation;
        )",
        R"(
┌─info.partition_id─┬─info.min_block─┬─info.max_block─┬─info.level─┬─info.mutation─┐
│ all               │             12 │             25 │          7 │             4 │
└───────────────────┴────────────────┴────────────────┴────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_info = {25, 6};
    FunctionDocumentation::Category category_info = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation_info = {description_info, syntax_info, arguments_info, {}, returned_value_info, examples_info, introduced_in_info, category_info};

    factory.registerFunction<FunctionMergeTreePartInfo>(documentation_info, FunctionFactory::Case::Insensitive);
}

}
