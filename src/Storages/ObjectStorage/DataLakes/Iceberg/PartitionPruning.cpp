#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

using namespace DB;

namespace Iceberg
{

Iceberg::PartitionTransformType getTransform(const String & transform_name)
{
    if (transform_name == "year" || transform_name == "years")
    {
        return Iceberg::PartitionTransformType::Year;
    }
    else if (transform_name == "month" || transform_name == "months")
    {
        return Iceberg::PartitionTransformType::Month;
    }
    else if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
    {
        return Iceberg::PartitionTransformType::Day;
    }
    else if (transform_name == "hour" || transform_name == "hours")
    {
        return Iceberg::PartitionTransformType::Hour;
    }
    else if (transform_name == "identity")
    {
        return Iceberg::PartitionTransformType::Identity;
    }
    else if (transform_name == "void")
    {
        return Iceberg::PartitionTransformType::Void;
    }
    else if (transform_name.starts_with("truncate"))
    {
        return Iceberg::PartitionTransformType::Truncate;
    }
    else
    {
        return Iceberg::PartitionTransformType::Unsupported;
    }
}

DB::ASTPtr getASTFromTransform(const String & transform_name, const String & column_name)
{
    std::shared_ptr<DB::ASTFunction> function = std::make_shared<ASTFunction>();
    function->arguments = std::make_shared<DB::ASTExpressionList>();
    function->children.push_back(function->arguments);

    if (transform_name == "year" || transform_name == "years")
    {
        function->name = "toYearNumSinceEpoch";
    }
    else if (transform_name == "month" || transform_name == "months")
    {
        function->name = "toMonthNumSinceEpoch";
    }
    else if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
    {
        function->name = "toRelativeDayNum";
    }
    else if (transform_name == "hour" || transform_name == "hours")
    {
        function->name = "toRelativeHourNum";
    }
    else if (transform_name == "identity")
    {
        return std::make_shared<ASTIdentifier>(column_name);
    }
    else if (transform_name.starts_with("truncate"))
    {
        function->name = "icebergTruncate";
        auto argument_start = transform_name.find('[');
        auto argument_width = transform_name.length() - 1 - argument_start;
        std::string width = transform_name.substr(argument_start + 1, argument_width);
        size_t truncate_width = DB::parse<size_t>(width);
        function->arguments->children.push_back(std::make_shared<DB::ASTLiteral>(truncate_width));
    }
    else if (transform_name == "void")
    {
        function->name = "tuple";
        return function;
    }
    else
    {
        return nullptr;
    }

    function->arguments->children.push_back(std::make_shared<DB::ASTIdentifier>(column_name));
    return function;
}

// This function is used to convert the value to the start of the corresponding time period (internal ClickHouse representation)
DateLUTImpl::Values getDateLUTImplValues(Int32 value, Iceberg::PartitionTransformType transform)
{
    switch (transform)
    {
        case Iceberg::PartitionTransformType::Year:
            return DateLUT::instance().lutIndexByYearSinceEpochStartsZeroIndexing(value);
        case Iceberg::PartitionTransformType::Month:
            return DateLUT::instance().lutIndexByMonthSinceEpochStartsZeroIndexing(static_cast<UInt32>(value));
        case Iceberg::PartitionTransformType::Day:
            return DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value));
        case Iceberg::PartitionTransformType::Hour:
        {
            DateLUTImpl::Values values = DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value / 24));
            values.date += (value % 24) * 3600;
            return values;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform for get values function: {}", transform);
    }
}

// This function is used to convert the value to the start of the corresponding time period (in seconds)
Int64 getTime(Int32 value, Iceberg::PartitionTransformType transform)
{
    DateLUTImpl::Values values = getDateLUTImplValues(value, transform);
    return values.date;
}

// This function is used to convert the value to the start of the corresponding date period (in days)
Int16 getDay(Int32 value, Iceberg::PartitionTransformType transform)
{
    DateLUTImpl::Time got_time = getTime(value, transform);
    return DateLUT::instance().toDayNum(got_time);
}


Range getPartitionRange(
    Iceberg::PartitionTransformType partition_transform, size_t index, ColumnPtr partition_column, DataTypePtr column_data_type)
{
    if (partition_transform == Iceberg::PartitionTransformType::Unsupported)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform: {}", partition_transform);
    }
    if (partition_transform == Iceberg::PartitionTransformType::Identity)
    {
        Field entry = (*partition_column.get())[index];
        return Range{entry, true, entry, true};
    }
    if (partition_column->getDataType() != TypeIndex::Int32)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition column type: {}", partition_column->getFamilyName());
    }

    auto nested_data_type = removeNullable(column_data_type);

    const auto * casted_innner_column = assert_cast<const ColumnInt32 *>(partition_column.get());
    Int32 value = casted_innner_column->getElement(index);
    LOG_DEBUG(&Poco::Logger::get("DEBUG"), "INDEX {}, VALUE {}", index, value);
    if ((WhichDataType(nested_data_type).isDate() || WhichDataType(nested_data_type).isDate32())
        && (partition_transform != Iceberg::PartitionTransformType::Hour))
    {
        const UInt16 begin_range_value = getDay(value, partition_transform);
        const UInt16 end_range_value = getDay(value + 1, partition_transform);
        LOG_DEBUG(&Poco::Logger::get("DEBUG"), "BEGIN {}, END {}", begin_range_value, end_range_value);
        return Range{begin_range_value, true, end_range_value, false};
    }
    else if (WhichDataType(nested_data_type).isDateTime64() || WhichDataType(nested_data_type).isDateTime())
    {
        const UInt64 begin_range_value = getTime(value, partition_transform);
        const UInt64 end_range_value = getTime(value + 1, partition_transform);
        LOG_DEBUG(&Poco::Logger::get("DEBUG"), "BEGIN {}, END {}", begin_range_value, end_range_value);
        return Range{begin_range_value, true, end_range_value, false};
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Partition transform {} is not supported for the type: {}", partition_transform, nested_data_type);
    }
}


}
