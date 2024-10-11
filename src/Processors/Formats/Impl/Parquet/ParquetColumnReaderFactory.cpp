#include "ParquetColumnReaderFactory.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>
#include <parquet/column_reader.h>


namespace DB
{
template <parquet::Type::type physical_type, TypeIndex target_type, bool dict>
SelectiveColumnReaderPtr createColumnReader(
    std::unique_ptr<LazyPageReader> /*page_reader*/, const ScanSpec & /*scan_spec*/, const parquet::LogicalType & /*logical_type*/)
{
    throw DB::Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "ParquetColumnReaderFactory::createColumnReader: not implemented for physical type {} and target type {}",
        magic_enum::enum_name(physical_type),
        magic_enum::enum_name(target_type));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType & /*logical_type*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt64, Int64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType & /*logical_type*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt64, Int64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt64>());
}

static UInt32 getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit)
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
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType & logical_type)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(3);
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime64, Int64>>(
        std::move(page_reader), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType & logical_type)
{
    DataTypePtr type_datetime64;
    if (logical_type.is_timestamp())
    {
        const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(logical_type);
        type_datetime64 = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
    }
    else
        type_datetime64 = std::make_shared<DataTypeDateTime64>(0);
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime64, Int64>>(
        std::move(page_reader), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt16, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt16, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt32, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt32, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate32, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate32, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat32, Float32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat32, Float32>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat64, Float64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat64, Float64>>(
        std::move(page_reader), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<StringDirectReader>(
        std::move(page_reader), scan_spec);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(
    std::unique_ptr<LazyPageReader> page_reader, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<StringDictionaryReader>(
        std::move(page_reader), scan_spec);
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
    target_type_ = target_type;
    return *this;
}

ParquetColumnReaderFactory::Builder & ParquetColumnReaderFactory::Builder::pageReader(std::unique_ptr<LazyPageReader> page_reader)
{
    page_reader_ = std::move(page_reader);
    return *this;
}

SelectiveColumnReaderPtr ParquetColumnReaderFactory::Builder::build()
{
    if (!column_descriptor_ || !page_reader_ || !target_type_)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "ParquetColumnReaderFactory::Builder: column descriptor, page reader and target type must be set");
    ScanSpec scan_spec{.column_name = column_descriptor_->name(), .column_desc = column_descriptor_, .filter = filter_};
    parquet::Type::type physical_type = column_descriptor_->physical_type();
    TypeIndex target_type = target_type_->getTypeId();
    const auto& logical_type = *column_descriptor_->logical_type();
    SelectiveColumnReaderPtr leaf_reader = nullptr;
    if (physical_type == parquet::Type::INT64)
    {
        if (isInt64(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isDateTime64(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isDateTime(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(std::move(page_reader_), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::INT32)
    {
        if (isInt16(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isInt32(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isDate32(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isDate(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(std::move(page_reader_), scan_spec, logical_type);
        }
        else if (isDateTime(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(std::move(page_reader_), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::FLOAT)
    {
        if (isFloat(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(std::move(page_reader_), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::DOUBLE)
    {
        if (isFloat(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(std::move(page_reader_), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::BYTE_ARRAY)
    {
        if (isString(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(std::move(page_reader_), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(std::move(page_reader_), scan_spec, logical_type);
        }
    }
    if (!leaf_reader)
    {
        throw DB::Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "ParquetColumnReaderFactory::createColumnReader: not implemented for physical type {} and target type {}",
            magic_enum::enum_name(physical_type),
            magic_enum::enum_name(target_type));
    }
    if (nullable_)
        return std::make_shared<OptionalColumnReader>(scan_spec, leaf_reader);
    else
        return leaf_reader;
}
ParquetColumnReaderFactory::Builder ParquetColumnReaderFactory::builder()
{
    return ParquetColumnReaderFactory::Builder();
}
}
