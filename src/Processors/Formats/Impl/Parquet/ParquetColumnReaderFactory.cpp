#include "ParquetColumnReaderFactory.h"
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Processors/Formats/Impl/Parquet/RowGroupChunkReader.h>
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>
#include <parquet/column_reader.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event ParquetFetchWaitTimeMicroseconds;
}

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
}

namespace DB
{

namespace
{
/// Create a column reader for a specific physical type and target type.
/// \tparam physical_type physical type in parquet file
/// \tparam target_type target type in ClickHouse
/// \tparam dict is dictionary column or not
/// \param page_reader_creator creator of page reader
/// \param scan_spec scan description
/// \param logical_type logical type in parquet file
/// \param type_hint type hint for some parquet file didn't have logical type
/// return
template <parquet::Type::type physical_type, TypeIndex target_type, bool dict>
SelectiveColumnReaderPtr createColumnReader(
    PageReaderCreator page_reader_creator [[maybe_unused]],
    const ScanSpec & scan_spec [[maybe_unused]],
    const parquet::LogicalType & logical_type [[maybe_unused]],
    const DataTypePtr type_hint [[maybe_unused]])
{
    throw DB::Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "ParquetColumnReaderFactory::createColumnReader: not implemented for physical type {} and target type {}",
        magic_enum::enum_name(physical_type),
        magic_enum::enum_name(target_type));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::UInt64, true>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeUInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::UInt64, false>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeUInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT96, TypeIndex::Int64, true>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT96, TypeIndex::Int64, false>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & /*logical_type*/,
    const DataTypePtr /*type_hint*/)
{
    return std::make_shared<FixedLengthColumnDirectReader<DataTypeInt64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
}

UInt32 getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit)
{
    switch (tm_unit)
    {
        case parquet::LogicalType::TimeUnit::MILLIS:
            return 3;
        case parquet::LogicalType::TimeUnit::MICROS:
            return 6;
        case parquet::LogicalType::TimeUnit::NANOS:
            return 9;
        default:
            throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, ", invalid timestamp unit: {}", tm_unit);
    }
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, false>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & logical_type,
    const DataTypePtr /*type_hint*/)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(0);
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime64, Int64>>(
        std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & logical_type,
    const DataTypePtr /*type_hint*/)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(0);
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime64, Int64>>(std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT96, TypeIndex::DateTime64, false>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & logical_type,
    const DataTypePtr /*type_hint*/)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(9);
    return std::make_shared<FixedLengthColumnDirectReader<DataTypeDateTime64>>(std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT96, TypeIndex::DateTime64, true>(
    PageReaderCreator page_reader_creator,
    const ScanSpec & scan_spec,
    const parquet::LogicalType & logical_type,
    const DataTypePtr /*type_hint*/)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(9);
    return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeDateTime64, DateTime64>>(
        std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int8, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt8, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt8>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int8, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt8, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt8>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt32>());
}


template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt8, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeUInt8, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt8>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BOOLEAN, TypeIndex::UInt8, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<BooleanColumnReader>(std::move(page_reader_creator), scan_spec);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt8, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeUInt8, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt8>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt16, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeUInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt16, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeUInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeUInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::UInt32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeUInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeUInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat32, Float32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat32, Float32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat64, Float64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat64, Float64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<StringDirectReader>(std::move(page_reader_creator), scan_spec);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    return std::make_shared<StringDictionaryReader>(std::move(page_reader_creator), scan_spec);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::FixedString, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto length = scan_spec.column_desc->type_length();
    return std::make_shared<FixedLengthColumnDirectReader<DataTypeFixedString>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFixedString>(length));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::FixedString, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto length = scan_spec.column_desc->type_length();
    return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeFixedString, String>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFixedString>(length));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::Decimal128, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    auto type_length = scan_spec.column_desc->type_length();
    if (precision <= 9 && type_length == 4)
        return std::make_shared<FixedLengthColumnDirectReader<DataTypeDecimal32>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal32>(precision, scale));
    else if (precision <= 18 && type_length == 8)
        return std::make_shared<FixedLengthColumnDirectReader<DataTypeDecimal64>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal64>(precision, scale));
    else if (precision <= 38 && precision > 18)
        return std::make_shared<FixedLengthColumnDirectReader<DataTypeDecimal128>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal128>(precision, scale));
    else if (precision <= 76 && precision > 38)
        return std::make_shared<FixedLengthColumnDirectReader<DataTypeDecimal256>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal256>(precision, scale));
    else
        throw DB::Exception(
            ErrorCodes::PARQUET_EXCEPTION, "ParquetColumnReaderFactory: unsupported precision {} type length {}", precision, type_length);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::Decimal128, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    auto type_length = scan_spec.column_desc->type_length();
    if (precision <= 9 && type_length == 4)
        return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeDecimal32, Decimal32>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal32>(precision, scale));
    else if (precision <= 18 && type_length == 8)
        return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeDecimal64, Decimal64>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal64>(precision, scale));
    else if (precision <= 38 && precision > 18)
        return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeDecimal128, Decimal128>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal128>(precision, scale));
    else if (precision <= 76 && precision > 38)
        return std::make_shared<FixedLengthColumnDictionaryReader<DataTypeDecimal256, Decimal256>>(
            std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal256>(precision, scale));
    else
        throw DB::Exception(
            ErrorCodes::PARQUET_EXCEPTION, "ParquetColumnReaderFactory: unsupported precision {} type length {}", precision, type_length);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Decimal32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    return std::make_shared<NumberColumnDirectReader<DataTypeDecimal32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal32>(precision, scale));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Decimal32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    return std::make_shared<NumberDictionaryReader<DataTypeDecimal32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal32>(precision, scale));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Decimal64, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    return std::make_shared<NumberColumnDirectReader<DataTypeDecimal64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal64>(precision, scale));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Decimal64, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &, const DataTypePtr /*type_hint*/)
{
    auto precision = scan_spec.column_desc->type_precision();
    auto scale = scan_spec.column_desc->type_scale();
    return std::make_shared<NumberDictionaryReader<DataTypeDecimal64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDecimal64>(precision, scale));
}
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::isOptional(const bool is_optional)
{
    is_optional_ = is_optional;
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::columnDescriptor(const parquet::ColumnDescriptor * columnDescr)
{
    column_descriptor_ = columnDescr;
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::dictionary(bool dictionary)
{
    dictionary_ = dictionary;
    return *this;
}
ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::nullable(bool nullable)
{
    nullable_ = nullable;
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::filter(const ColumnFilterPtr & filter)
{
    filter_ = filter;
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::targetType(const DataTypePtr & target_type)
{
    target_type_ = removeNullable(target_type);
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::pageReader(PageReaderCreator page_reader_creator_)
{
    page_reader_creator = page_reader_creator_;
    return *this;
}

ParquetColumnReaderFactory::Builder &
ParquetColumnReaderFactory::Builder::columnChunkMeta(std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk_meta)
{
    column_chunk_meta_ = std::move(column_chunk_meta);
    return *this;
}

SelectiveColumnReaderPtr ParquetColumnReaderFactory::Builder::build()
{
    if (!column_descriptor_ || !page_reader_creator || !target_type_)
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR, "ParquetColumnReaderFactory::Builder: column descriptor, page reader and target type must be set");
    ScanSpec scan_spec{.column_name = column_descriptor_->name(), .column_desc = column_descriptor_, .filter = filter_};
    parquet::Type::type physical_type = column_descriptor_->physical_type();
    auto converted_type = column_descriptor_->converted_type();
    TypeIndex target_type = target_type_->getTypeId();
    const auto & logical_type = *column_descriptor_->logical_type();
    SelectiveColumnReaderPtr leaf_reader = nullptr;
    if (physical_type == parquet::Type::INT64)
    {
        // for clickbench test
        if (isDateTime(target_type) && converted_type == parquet::ConvertedType::NONE)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (
            converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS || converted_type == parquet::ConvertedType::TIMESTAMP_MICROS
            || converted_type == parquet::ConvertedType::TIME_MILLIS || converted_type == parquet::ConvertedType::TIME_MICROS
            || logical_type.is_timestamp())
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::UINT_64)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::UInt64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::UInt64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::INT_64 || converted_type == parquet::ConvertedType::NONE)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::DECIMAL)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Decimal64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Decimal64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
    }
    else if (physical_type == parquet::Type::INT32)
    {
        // for clickbench test
        if (isDateTime(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::INT_8)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int8, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int8, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::INT_16)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::INT_32 || converted_type == parquet::ConvertedType::NONE)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::UINT_8)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt8, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt8, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::UINT_16)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt16, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt16, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::UINT_32)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt32, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::UInt32, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (isDate(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::DATE)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::DECIMAL)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Decimal32, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Decimal32, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
    }
    else if (physical_type == parquet::Type::BOOLEAN)
    {
        leaf_reader = createColumnReader<parquet::Type::BOOLEAN, TypeIndex::UInt8, false>(
            std::move(page_reader_creator), scan_spec, logical_type, target_type_);
    }
    else if (physical_type == parquet::Type::FLOAT)
    {
        if (dictionary_)
            leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(
                std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        else
            leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(
                std::move(page_reader_creator), scan_spec, logical_type, target_type_);
    }
    else if (physical_type == parquet::Type::DOUBLE)
    {
        if (dictionary_)
            leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(
                std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        else
            leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(
                std::move(page_reader_creator), scan_spec, logical_type, target_type_);
    }
    else if (physical_type == parquet::Type::BYTE_ARRAY)
    {
        if (isString(target_type) || converted_type == parquet::ConvertedType::UTF8)
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
    }
    else if (physical_type == parquet::Type::FIXED_LEN_BYTE_ARRAY)
    {
        if (converted_type == parquet::ConvertedType::DECIMAL)
        {
            // datatype will choose by type precision and scale, current support decimal128 and decimal256
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::Decimal128, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::Decimal128, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::FixedString, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY, TypeIndex::FixedString, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
    }
    else if (physical_type == parquet::Type::INT96)
    {
        if (converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS || converted_type == parquet::ConvertedType::TIMESTAMP_MICROS
            || converted_type == parquet::ConvertedType::TIME_MILLIS || converted_type == parquet::ConvertedType::TIME_MICROS
            || logical_type.is_timestamp() || (converted_type == parquet::ConvertedType::NONE && isDateTime64(target_type)))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT96, TypeIndex::DateTime64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT96, TypeIndex::DateTime64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
        else if (converted_type == parquet::ConvertedType::NONE && isInt64(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT96, TypeIndex::Int64, true>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
            else
                leaf_reader = createColumnReader<parquet::Type::INT96, TypeIndex::Int64, false>(
                    std::move(page_reader_creator), scan_spec, logical_type, target_type_);
        }
    }
    if (!leaf_reader)
    {
        throw DB::Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "ParquetColumnReaderFactory::createColumnReader: not implemented for physical type {}, convert type {} and target type {}",
            magic_enum::enum_name(physical_type),
            magic_enum::enum_name(converted_type),
            magic_enum::enum_name(target_type));
    }
    leaf_reader->setColumnChunkMeta(std::move(column_chunk_meta_));
    if (nullable_)
        return std::make_shared<OptionalColumnReader>(scan_spec, leaf_reader, is_optional_);
    else
        return leaf_reader;
}

ParquetColumnReaderFactory::Builder ParquetColumnReaderFactory::builder()
{
    return ParquetColumnReaderFactory::Builder();
}

/// Returns true if repeated type is an element type for the list.
/// Used to determine legacy list types.
/// This method is copied from Spark Parquet reader and is based on the reference:
/// <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
///   #backward-compatibility-rules
bool isListElement(parquet::schema::Node & node)
{
    // For legacy 2-level list types with primitive element type, e.g.:
    //
    //    // ARRAY<INT> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated int32 element;
    //    }
    //
    return node.is_primitive() ||
        // For legacy 2-level list types whose element type is a group type with 2 or more
        // fields, e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group element {
        //        required binary str (UTF8);
        //        required int32 num;
        //      };
        //    }
        //
        (node.is_group() && static_cast<parquet::schema::GroupNode &>(node).field_count() > 1) ||
        // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0),
        // e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group array {
        //        required binary str (UTF8);
        //      };
        //    }
        //
        node.name() == "array" ||
        // For Parquet data generated by parquet-thrift, e.g.:
        //
        //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
        //    optional group my_list (LIST) {
        //      repeated group my_list_tuple {
        //        required binary str (UTF8);
        //      };
        //    }
        //
        node.name().ends_with("_tuple");
}

static std::shared_ptr<parquet::schema::GroupNode> checkAndGetGroupNode(parquet::schema::NodePtr node)
{
    if (!node)
        return nullptr;
    if (!node->is_group())
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "need group node");
    return std::static_pointer_cast<parquet::schema::GroupNode>(node);
}

SelectiveColumnReaderPtr
ColumnReaderBuilder::buildReader(parquet::schema::NodePtr node, const DataTypePtr & target_type, int def_level, int rep_level, bool is_key)
{
    if (node->repetition() == parquet::Repetition::UNDEFINED)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Undefined repetition level");
    if (!node->is_required())
        def_level++;
    if (node->is_repeated())
        rep_level++;
    if (node->is_primitive())
    {
        auto full_name = node->path()->ToDotString();
        int column_idx = context.parquet_reader->metaData().schema()->ColumnIndex(*node);
        RowGroupPrefetchPtr row_group_prefetch;
        bool conditions = predicate_columns.contains(full_name);
        if (conditions)
            row_group_prefetch = context.prefetch_conditions;
        else
            row_group_prefetch = context.prefetch;
        auto column_chunk_meta = context.row_group_meta->ColumnChunk(column_idx);
        auto column_range = getColumnRange(*column_chunk_meta);
        row_group_prefetch->prefetchRange(column_range);
        PageReaderCreator creator = [&, conditions, column_idx, column_range]
        {
            Stopwatch time;
            ColumnChunkData data;
            if (conditions)
            {
                context.prefetch_conditions->startPrefetch();
                data = context.prefetch_conditions->readRange(column_range);
            }
            else
            {
                context.prefetch->startPrefetch();
                data = context.prefetch->readRange(column_range);
            }
            auto page_reader = std::make_unique<LazyPageReader>(
                std::make_unique<ReadBufferFromMemory>(reinterpret_cast<char *>(data.data), data.size),
                context.parquet_reader->readerProperties(),
                context.row_group_meta->ColumnChunk(column_idx)->compression(),
                column_range.offset);
            ProfileEvents::increment(ProfileEvents::ParquetFetchWaitTimeMicroseconds, time.elapsedMicroseconds());
            return page_reader;
        };
        const auto * column_desc = context.parquet_reader->metaData().schema()->Column(column_idx);

        // in some case, dictionary is not set in column chunk, but set in encoding
        bool dictionary = column_chunk_meta->has_dictionary_page();
        auto encodings = column_chunk_meta->encodings();
        for (const auto encoding : encodings)
        {
            if (encoding == parquet::Encoding::PLAIN_DICTIONARY || encoding == parquet::Encoding::RLE_DICTIONARY)
                dictionary = true;
        }
        auto leaf_reader
            = ParquetColumnReaderFactory::builder()
                  .nullable(!is_key && (target_type->isNullable() || node->is_optional() || column_desc->max_definition_level() > 0))
                  .isOptional(node->is_optional())
                  .dictionary(dictionary)
                  .columnDescriptor(column_desc)
                  .pageReader(creator)
                  .targetType(target_type)
                  .filter(inplace_filter_mapping.contains(full_name) ? inplace_filter_mapping.at(full_name) : nullptr)
                  .columnChunkMeta(std::move(column_chunk_meta))
                  .build();
        if (context.row_group_index_reader)
        {
            if (column_idx >= 0)
            {
                leaf_reader->setColumnIndex(context.row_group_index_reader->GetColumnIndex(column_idx));
                leaf_reader->setOffsetIndex(context.row_group_index_reader->GetOffsetIndex(column_idx));
            }
        }
        return leaf_reader;
    }
    else if (node->converted_type() == parquet::ConvertedType::LIST)
    {
        auto group_node = checkAndGetGroupNode(node);
        if (group_node->field_count() != 1)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "List group node must have exactly one field");
        auto repeated_field = group_node->field(0);
        if (isListElement(*repeated_field))
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(target_type.get());
            auto reader = buildReader(repeated_field, array_type->getNestedType(), def_level + 1, rep_level + 1);
            return std::make_shared<ListColumnReader>(rep_level, def_level, reader);
        }
        else
        {
            auto child_field = std::static_pointer_cast<parquet::schema::GroupNode>(repeated_field)->field(0);
            const auto * array_type = checkAndGetDataType<DataTypeArray>(target_type.get());
            auto reader = buildReader(child_field, array_type->getNestedType(), def_level + 1, rep_level + 1);
            return std::make_shared<ListColumnReader>(rep_level, def_level, reader);
        }
    }
    else if (node->converted_type() == parquet::ConvertedType::MAP || node->converted_type() == parquet::ConvertedType::MAP_KEY_VALUE)
    {
        auto map_node = checkAndGetGroupNode(node);
        if (map_node->field_count() != 1)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Map group node must have exactly one field");
        auto key_value_node = checkAndGetGroupNode(map_node->field(0));
        if (key_value_node->field_count() != 2)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Map key-value group node must have exactly two fields");
        if (!key_value_node->field(0)->is_primitive())
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Map key field must be primitive");

        const auto & map_type = checkAndGetDataType<DataTypeMap>(*target_type);
        auto key_value_types
            = checkAndGetDataType<DataTypeTuple>(*checkAndGetDataType<DataTypeArray>(*map_type.getNestedType()).getNestedType())
                  .getElements();
        auto key_reader = buildReader(key_value_node->field(0), key_value_types.front(), def_level + 1, rep_level + 1, true);
        auto value_reader = buildReader(key_value_node->field(1), key_value_types.back(), def_level + 1, rep_level + 1);
        return std::make_shared<MapColumnReader>(rep_level, def_level, key_reader, value_reader);
    }
    // Structure type
    else
    {
        auto struct_node = checkAndGetGroupNode(node);
        const auto * struct_type = checkAndGetDataType<DataTypeTuple>(target_type.get());
        if (!struct_type)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Target type for node {} must be DataTypeTuple", struct_node->name());
        auto names = struct_type->getElementNames();
        int child_num = struct_node->field_count();
        std::unordered_map<String, SelectiveColumnReaderPtr> readers;
        for (const auto & name : names)
        {
            for (int i = 0; i < child_num; ++i)
            {
                if (struct_node->field(i)->name() == name)
                {
                    auto child_field = struct_node->field(i);
                    auto child_type = struct_type->getElements().at(i);
                    auto reader = buildReader(child_field, child_type, def_level, rep_level);
                    readers.emplace(name, reader);
                }
            }
            if (!readers.contains(name))
            {
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "{} not found in struct node {}", name, struct_node->name());
            }
        }
        return std::make_shared<StructColumnReader>(rep_level, def_level, readers, target_type);
    }
}
ColumnReaderBuilder::ColumnReaderBuilder(
    const Block & requiredColumns_,
    const RowGroupContext & context_,
    const std::unordered_map<String, ColumnFilterPtr> & inplaceFilterMapping_,
    const std::unordered_set<String> & predicateColumns_)
    : required_columns(requiredColumns_)
    , context(context_)
    , inplace_filter_mapping(inplaceFilterMapping_)
    , predicate_columns(predicateColumns_)
{
}
}
