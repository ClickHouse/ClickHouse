#pragma once

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesCache.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteIntText.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Consumer>
void enumerateJSONValues(
    const ColumnObject & column_object,
    const DataTypeObject & type_object,
    Consumer & consumer,
    size_t start_row = 0,
    size_t num_rows = std::numeric_limits<size_t>::max())
{
    struct PathInfo
    {
        std::string_view path;
        DataTypePtr type;
        String type_name;
        const IColumn * column = nullptr;
        SerializationPtr serialization;
        bool is_dynamic = false;
        bool is_nullable = false;
        ColumnVariant::Discriminator cached_discriminator = ColumnVariant::NULL_DISCRIMINATOR;
        const IDataType * cached_dynamic_type = nullptr;
        SerializationPtr cached_dynamic_serialization{};
    };

    const auto & typed_path_types = type_object.getTypedPaths();
    const auto & typed_path_columns = column_object.getTypedPaths();
    const auto & dynamic_path_columns = column_object.getDynamicPaths();
    VectorWithMemoryTracking<PathInfo> sorted_paths;
    sorted_paths.reserve(typed_path_types.size() + dynamic_path_columns.size());

    for (const auto & [path, type] : typed_path_types)
    {
        const auto & column = typed_path_columns.at(path);
        const auto value_type = removeNullableOrLowCardinalityNullable(type);
        const bool is_dynamic = DB::isDynamic(value_type);
        sorted_paths.push_back({
            path,
            value_type,
            is_dynamic ? String{} : value_type->getName(),
            column.get(),
            type->getDefaultSerialization(),
            is_dynamic,
            canContainNull(*type)});
    }

    for (const auto & [path, column] : dynamic_path_columns)
        sorted_paths.push_back({path, nullptr, {}, column.get(), nullptr, true, false});

    std::sort(sorted_paths.begin(), sorted_paths.end(), [](const PathInfo & lhs, const PathInfo & rhs) { return lhs.path < rhs.path; });

    UnorderedMapWithMemoryTracking<String, SerializationPtr> serializations_cache;
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> shared_columns_cache;

    const auto & shared_data_offsets = column_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
    const FormatSettings format_settings;

    auto consume_shared = [&](std::string_view path, std::string_view value_data)
    {
        if (!consumer.shouldConsumePath(path))
            return;

        ReadBufferFromMemory buffer(value_data);

        char type_index = 0;
        if (!buffer.peek(type_index))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse shared data value of JSON: no type index found");

        DataTypePtr type;
        SerializationPtr serialization;
        const auto & cache = getSimpleDataTypesCache();
        const auto binary_type_index = static_cast<BinaryTypeIndex>(type_index);
        const bool has_cached_type = cache.hasElement(binary_type_index);

        if (has_cached_type && !consumer.shouldConsumeValue(path, *cache.getElement(binary_type_index).type))
            return;

        if (binary_type_index == BinaryTypeIndex::String)
        {
            ++buffer.position();
            size_t size = 0;
            readVarUInt(size, buffer);
            if (size > DEFAULT_MAX_STRING_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");
            if (size > buffer.available())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse shared String value of JSON");
            consumer.consumeSharedScalar(path, binary_type_index, std::string_view(buffer.position(), size));
            return;
        }
        if (binary_type_index == BinaryTypeIndex::Bool)
        {
            ++buffer.position();
            UInt8 value = 0;
            readBinary(value, buffer);
            consumer.consumeSharedScalar(
                path,
                binary_type_index,
                value ? format_settings.bool_true_representation : format_settings.bool_false_representation);
            return;
        }
        if (binary_type_index == BinaryTypeIndex::Int64)
        {
            ++buffer.position();
            Int64 value = 0;
            readBinary(value, buffer);
            char text[max_int_width<Int64>];
            const char * end = itoa(value, text);
            consumer.consumeSharedScalar(path, binary_type_index, std::string_view(text, end - text));
            return;
        }

        /// Resolve the type name once per value: `IDataType::getName` builds a String.
        const String * type_name = nullptr;
        String decoded_type_name;
        if (has_cached_type)
        {
            ++buffer.position();
            const auto & element = cache.getElement(binary_type_index);
            type = element.type;
            serialization = element.serialization;
            type_name = &element.name;
        }
        else
        {
            type = decodeDataType(buffer);
            decoded_type_name = type->getName();
            type_name = &decoded_type_name;
            auto [it, inserted] = serializations_cache.try_emplace(decoded_type_name);
            if (inserted)
                it->second = type->getDefaultSerialization();
            serialization = it->second;
        }

        if (isNothing(type))
            return;
        if (!has_cached_type && !consumer.shouldConsumeValue(path, *type))
            return;

        auto [column_it, inserted] = shared_columns_cache.try_emplace(*type_name);
        if (inserted)
            column_it->second = type->createColumn();

        auto & temporary_column = *column_it->second;
        serialization->deserializeBinary(temporary_column, buffer, format_settings);
        consumer.consumeValue(path, *type, *type_name, *serialization, temporary_column, 0, true, format_settings);
        temporary_column.popBack(1);
    };

    auto consume_path = [&](PathInfo & entry, size_t row)
    {
        if (!consumer.shouldConsumePath(entry.path))
            return;

        if ((entry.is_dynamic || entry.is_nullable) && entry.column->isNullAt(row))
        {
            consumer.consumeNull(entry.path, entry.is_nullable);
            return;
        }

        if (!entry.is_dynamic)
        {
            consumer.consumeValue(
                entry.path, *entry.type, entry.type_name, *entry.serialization, *entry.column, row, false, format_settings);
            return;
        }

        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*entry.column);
        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        const size_t variant_row = variant_column.offsetAt(row);

        if (discriminator == dynamic_column.getSharedVariantDiscriminator())
        {
            consume_shared(entry.path, dynamic_column.getSharedVariant().getDataAt(variant_row));
            return;
        }

        const auto & type_name = dynamic_column.getVariantInfo().variant_names[discriminator];
        if (entry.cached_discriminator != discriminator)
        {
            entry.cached_discriminator = discriminator;
            entry.cached_dynamic_type
                = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type).getVariant(discriminator).get();
            auto [serialization_it, inserted] = serializations_cache.try_emplace(type_name);
            if (inserted)
                serialization_it->second = entry.cached_dynamic_type->getDefaultSerialization();
            entry.cached_dynamic_serialization = serialization_it->second;
        }

        consumer.consumeValue(
            entry.path,
            *entry.cached_dynamic_type,
            type_name,
            *entry.cached_dynamic_serialization,
            variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_row,
            true,
            format_settings);
    };

    chassert(start_row <= shared_data_offsets.size());
    const size_t end_row = start_row + std::min(num_rows, shared_data_offsets.size() - start_row);

    for (auto & path : sorted_paths)
    {
        for (size_t row = start_row; row != end_row; ++row)
        {
            consumer.setRow(row - start_row);
            consume_path(path, row);
        }
    }

    for (size_t row = start_row; row != end_row; ++row)
    {
        consumer.setRow(row - start_row);
        const size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = shared_data_offsets[static_cast<ssize_t>(row)];
        for (size_t shared_index = start; shared_index != end; ++shared_index)
            consume_shared(shared_data_paths->getDataAt(shared_index), shared_data_values->getDataAt(shared_index));
    }

    consumer.finishRows(end_row - start_row);
}

}
