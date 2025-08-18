#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>

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
    static constexpr auto name = "isMergeTreePartCoveredBy";
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

class FunctionMergeTreePartInfo : public IFunction
{
public:
    static constexpr auto name = "mergeTreePartInfo";
    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMergeTreePartInfo>(); }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateFunctionArguments(*this, arguments, FunctionArgumentDescriptors{
            {"part_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isAnyStringType), nullptr, "String or FixedString or LowCardinality String"}
        });

        DataTypes types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt64>(),
        };

        Names names = {
            "partition_id",
            "min_block",
            "max_block",
            "level",
            "mutation",
        };

        return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * input_column = arguments[0].column.get();

        auto partition_column = ColumnString::create();
        auto min_block_column = ColumnInt64::create();
        auto max_block_column = ColumnInt64::create();
        auto level_column = ColumnInt64::create();
        auto mutation_column = ColumnInt64::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const MergeTreePartInfo part_info = constructPartInfo(input_column->getDataAt(i).toView());

            partition_column->insertData(part_info.getPartitionId().data(), part_info.getPartitionId().size());
            min_block_column->insertValue(part_info.min_block);
            max_block_column->insertValue(part_info.max_block);
            level_column->insertValue(part_info.level);
            mutation_column->insertValue(part_info.mutation);
        }

        return ColumnTuple::create(Columns{
            std::move(partition_column),
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
    factory.registerFunction<FunctionMergeTreePartCoverage>(
        FunctionDocumentation{
            .description = "Checks if one MergeTree part covers another",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection,
        },
        FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionMergeTreePartInfo>(
        FunctionDocumentation{
            .description = "Represents String value as a MergeTreePartInfo structure",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection,
        },
        FunctionFactory::Case::Insensitive);
}

}
