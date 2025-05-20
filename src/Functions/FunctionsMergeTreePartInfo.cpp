#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Common/register_objects.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

MergeTreePartInfo constructPartInfo(std::string_view data)
{
    if (auto info = MergeTreePartInfo::tryParsePartName(data, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
        return std::move(info.value());

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid MergeTree Part Name.");
}

bool isAnyStringType(const IDataType & data_type)
{
    return isStringOrFixedString(removeLowCardinality(data_type.getPtr()));
}

class FunctionMergeTreePartCoverage : public IFunction
{
    static MergeTreePartInfo constructCoveringPart(const IColumn * covering_column)
    {
        if (!isColumnConst(*covering_column) && covering_column->size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument should have single value.");

        return constructPartInfo(covering_column->getDataAt(0).toView());
    }

public:
    static constexpr auto name = "isPartCoveredBy";
    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMergeTreePartCoverage>(); }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateFunctionArguments(*this, arguments, FunctionArgumentDescriptors{
            {"nested_part", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"},
            {"covering_part", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"}
        });

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt8::create();
        const IColumn * input_column = arguments.front().column.get();
        const MergeTreePartInfo covering_part = constructCoveringPart(arguments[1].column.get());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const MergeTreePartInfo part_info = constructPartInfo(input_column->getDataAt(i).toView());
            result_column->insertValue(covering_part.contains(part_info));
        }

        return result_column;
    }
};

template <class NameHolder, class ReturnType, class ReturnColumn, class Extractor>
class FunctionMergeTreePartInfoExtractor : public IFunction
{
public:
    static constexpr auto name = NameHolder::name;
    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMergeTreePartInfoExtractor>(); }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateFunctionArguments(*this, arguments, FunctionArgumentDescriptors{
            {"part_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"}
        });

        return std::make_shared<ReturnType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_column = ReturnColumn::create();
        const IColumn * input_column = arguments[0].column.get();

        Extractor extractor;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const MergeTreePartInfo part_info = constructPartInfo(input_column->getDataAt(i).toView());
            extractor(part_info, *result_column);
        }

        return result_column;
    }
};

struct NamePartitionId { static constexpr auto name{"partPartitionId"}; };
struct NameMinBlock { static constexpr auto name{"partMinBlock"}; };
struct NameMaxBlock { static constexpr auto name{"partMaxBlock"}; };
struct NameMergeLevel { static constexpr auto name{"partMergeLevel"}; };
struct NameMutationLevel { static constexpr auto name{"partMutationLevel"}; };

}

REGISTER_FUNCTION(MergeTreePartInfoTools)
{
    constexpr static auto partition_id_extractor = [](const MergeTreePartInfo & part_info, ColumnString & result_column)
    {
        result_column.insertData(part_info.getPartitionId().data(), part_info.getPartitionId().size());
    };

    constexpr static auto min_block_extractor = [](const MergeTreePartInfo & part_info, ColumnInt64 & result_column)
    {
        result_column.insertValue(part_info.min_block);
    };

    constexpr static auto max_block_extractor = [](const MergeTreePartInfo & part_info, ColumnInt64 & result_column)
    {
        result_column.insertValue(part_info.max_block);
    };

    constexpr static auto merge_level_extractor = [](const MergeTreePartInfo & part_info, ColumnInt64 & result_column)
    {
        result_column.insertValue(part_info.level);
    };

    constexpr static auto mutation_extractor = [](const MergeTreePartInfo & part_info, ColumnInt64 & result_column)
    {
        result_column.insertValue(part_info.mutation);
    };

    factory.registerFunction<FunctionMergeTreePartInfoExtractor<NamePartitionId, DataTypeString, ColumnString, decltype(partition_id_extractor)>>();
    factory.registerFunction<FunctionMergeTreePartInfoExtractor<NameMinBlock, DataTypeInt64, ColumnInt64, decltype(min_block_extractor)>>();
    factory.registerFunction<FunctionMergeTreePartInfoExtractor<NameMaxBlock, DataTypeInt64, ColumnInt64, decltype(max_block_extractor)>>();
    factory.registerFunction<FunctionMergeTreePartInfoExtractor<NameMergeLevel, DataTypeInt64, ColumnInt64, decltype(merge_level_extractor)>>();
    factory.registerFunction<FunctionMergeTreePartInfoExtractor<NameMutationLevel, DataTypeInt64, ColumnInt64, decltype(mutation_extractor)>>();
    factory.registerFunction<FunctionMergeTreePartCoverage>();
}

}
