#include "ParquetColumnReaderFactory.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeArray.h>
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>
#include <Processors/Formats/Impl/Parquet/RowGroupChunkReader.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <parquet/column_reader.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event ParquetFetchWaitTimeMicroseconds;
}

namespace DB
{
template <parquet::Type::type physical_type, TypeIndex target_type, bool dict>
SelectiveColumnReaderPtr createColumnReader(
    PageReaderCreator /*page_reader_creator*/, const ScanSpec & /*scan_spec*/, const parquet::LogicalType & /*logical_type*/)
{
    throw DB::Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "ParquetColumnReaderFactory::createColumnReader: not implemented for physical type {} and target type {}",
        magic_enum::enum_name(physical_type),
        magic_enum::enum_name(target_type));
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType & /*logical_type*/)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType & /*logical_type*/)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt64, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt64>());
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
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType & logical_type)
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
        std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType & logical_type)
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
        std::move(page_reader_creator), scan_spec, type_datetime64);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt16, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt16>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeInt32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeInt32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate32, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDate, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDate, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDate>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeDateTime, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeDateTime, Int32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeDateTime>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat32, Float32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat32, Float32>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat32>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberColumnDirectReader<DataTypeFloat64, Float64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<NumberDictionaryReader<DataTypeFloat64, Float64>>(
        std::move(page_reader_creator), scan_spec, std::make_shared<DataTypeFloat64>());
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<StringDirectReader>(
        std::move(page_reader_creator), scan_spec);
}

template <>
SelectiveColumnReaderPtr createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(
    PageReaderCreator page_reader_creator, const ScanSpec & scan_spec, const parquet::LogicalType &)
{
    return std::make_shared<StringDictionaryReader>(
        std::move(page_reader_creator), scan_spec);
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

SelectiveColumnReaderPtr ParquetColumnReaderFactory::Builder::build()
{
    if (!column_descriptor_ || !page_reader_creator || !target_type_)
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
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::Int64, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isDateTime64(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime64, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isDateTime(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT64, TypeIndex::DateTime, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::INT32)
    {
        if (isInt16(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int16, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isInt32(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Int32, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isDate32(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date32, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isDate(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::Date, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
        else if (isDateTime(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::INT32, TypeIndex::DateTime, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::FLOAT)
    {
        if (isFloat(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::FLOAT, TypeIndex::Float32, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::DOUBLE)
    {
        if (isFloat(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::DOUBLE, TypeIndex::Float64, false>(std::move(page_reader_creator), scan_spec, logical_type);
        }
    }
    else if (physical_type == parquet::Type::BYTE_ARRAY)
    {
        if (isString(target_type))
        {
            if (dictionary_)
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, true>(std::move(page_reader_creator), scan_spec, logical_type);
            else
                leaf_reader = createColumnReader<parquet::Type::BYTE_ARRAY, TypeIndex::String, false>(std::move(page_reader_creator), scan_spec, logical_type);
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
        (node.is_group() && static_cast<parquet::schema::GroupNode&>(node).field_count() > 1) ||
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

SelectiveColumnReaderPtr createColumnReaderRecursive(const RowGroupContext& context, parquet::schema::NodePtr node, int def_level, int rep_level, bool condition_column, const ColumnFilterPtr & filter, const DataTypePtr & target_type)
{
    if (node->repetition() == parquet::Repetition::UNDEFINED)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Undefined repetition level");
    if (!node->is_required())
        def_level++;
    if (node->is_repeated())
        rep_level++;
    if (node->is_primitive())
    {
        int column_idx = context.parquet_reader->metaData().schema()->ColumnIndex(*node);
        RowGroupPrefetchPtr row_group_prefetch;
        if (condition_column)
            row_group_prefetch = context.prefetch_conditions;
        else
            row_group_prefetch = context.prefetch;
        auto column_range = getColumnRange(*context.row_group_meta->ColumnChunk(column_idx));
        row_group_prefetch->prefetchRange(column_range);
        PageReaderCreator creator = [&,row_group_prefetch , column_idx, column_range]
        {
            Stopwatch time;
            row_group_prefetch->startPrefetch();
            auto data = row_group_prefetch->readRange(column_range);
            auto page_reader = std::make_unique<LazyPageReader>(
                std::make_shared<ReadBufferFromMemory>(reinterpret_cast<char *>(data.data), data.size),
                context.parquet_reader->readerProperties(),
                context.row_group_meta->num_rows(),
                context.row_group_meta->ColumnChunk(column_idx)->compression());
            ProfileEvents::increment(ProfileEvents::ParquetFetchWaitTimeMicroseconds, time.elapsedMicroseconds());
            return page_reader;
        };
        const auto * column_desc = context.parquet_reader->metaData().schema()->Column(column_idx);

        return ParquetColumnReaderFactory::builder()
                            .nullable(node->is_optional())
                            .dictionary(context.row_group_meta->ColumnChunk(column_idx)->has_dictionary_page())
                            .columnDescriptor(column_desc)
                            .pageReader(std::move(creator))
                            .targetType(target_type)
                            .filter(filter)
                            .build();
    }
    else if (node->converted_type() == parquet::ConvertedType::LIST)
    {
        auto group_node = std::static_pointer_cast<parquet::schema::GroupNode>(node);
        if (group_node->field_count() != 1)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "List group node must have exactly one field");
        auto repeated_field = group_node->field(0);
        if (isListElement(*repeated_field))
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(target_type.get());
            auto reader = createColumnReaderRecursive(context, repeated_field, def_level, rep_level, condition_column, nullptr, array_type->getNestedType());
            return std::make_shared<ListColumnReader>(rep_level, def_level, reader);
        }
        else
        {
            auto child_field = std::static_pointer_cast<parquet::schema::GroupNode>(repeated_field)->field(0);
            const auto * array_type = checkAndGetDataType<DataTypeArray>(target_type.get());
            auto reader = createColumnReaderRecursive(context, child_field, def_level+1, rep_level+1, condition_column, nullptr, array_type->getNestedType());
            return std::make_shared<ListColumnReader>(rep_level, def_level, reader);
        }
    }
    return DB::SelectiveColumnReaderPtr();
}
}
