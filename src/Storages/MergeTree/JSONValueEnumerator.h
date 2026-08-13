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

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
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
        ColumnPtr owned_column;
        const IColumn * column = nullptr;
        SerializationPtr serialization;
        bool is_dynamic = false;
        bool is_nullable = false;
    };

    const auto & typed_path_types = type_object.getTypedPaths();
    const auto & typed_path_columns = column_object.getTypedPaths();
    const auto & dynamic_path_columns = column_object.getDynamicPaths();
    VectorWithMemoryTracking<PathInfo> sorted_paths;
    sorted_paths.reserve(typed_path_types.size() + dynamic_path_columns.size());

    for (const auto & [path, type] : typed_path_types)
    {
        const auto & column = typed_path_columns.at(path);
        const auto full_type = recursiveRemoveLowCardinality(type);
        const auto full_column = recursiveRemoveLowCardinality(column);
        const auto value_type = removeNullableOrLowCardinalityNullable(full_type);
        const bool is_dynamic = DB::isDynamic(value_type);
        sorted_paths.push_back({
            path,
            full_type,
            is_dynamic ? String{} : full_type->getName(),
            full_column,
            full_column.get(),
            full_type->getDefaultSerialization(),
            is_dynamic,
            canContainNull(*full_type)});
    }

    for (const auto & [path, column] : dynamic_path_columns)
        sorted_paths.push_back({path, nullptr, {}, column, column.get(), nullptr, true, false});

    std::sort(sorted_paths.begin(), sorted_paths.end(), [](const PathInfo & lhs, const PathInfo & rhs) { return lhs.path < rhs.path; });

    UnorderedMapWithMemoryTracking<String, SerializationPtr> serializations_cache;
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> shared_columns_cache;

    const auto & shared_data_offsets = column_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
    const FormatSettings format_settings;

    auto consume_shared = [&](std::string_view path, std::string_view value_data)
    {
        if constexpr (requires { consumer.shouldConsumePath(path); })
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

        if (cache.hasElement(binary_type_index))
        {
            ++buffer.position();
            const auto & element = cache.getElement(binary_type_index);
            type = element.type;
            serialization = element.serialization;
        }
        else
        {
            type = decodeDataType(buffer);
            auto [it, inserted] = serializations_cache.try_emplace(type->getName());
            if (inserted)
                it->second = type->getDefaultSerialization();
            serialization = it->second;
        }

        if (isNothing(type))
            return;

        auto [column_it, inserted] = shared_columns_cache.try_emplace(type->getName());
        if (inserted)
            column_it->second = type->createColumn();

        auto & temporary_column = *column_it->second;
        serialization->deserializeBinary(temporary_column, buffer, format_settings);
        consumer.consumeValue(path, *type, type->getName(), *serialization, temporary_column, 0, true, format_settings);
        temporary_column.popBack(1);
    };

    auto consume_path = [&](PathInfo & entry, size_t row)
    {
        if constexpr (requires { consumer.shouldConsumePath(entry.path); })
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

        const auto * type
            = assert_cast<const DataTypeVariant &>(*dynamic_column.getVariantInfo().variant_type).getVariant(discriminator).get();
        const auto & type_name = dynamic_column.getVariantInfo().variant_names[discriminator];
        auto [serialization_it, inserted] = serializations_cache.try_emplace(type_name);
        if (inserted)
            serialization_it->second = type->getDefaultSerialization();
        consumer.consumeValue(
            entry.path,
            *type,
            type_name,
            *serialization_it->second,
            variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_row,
            true,
            format_settings);
    };

    chassert(start_row <= shared_data_offsets.size());
    const size_t end_row = start_row + std::min(num_rows, shared_data_offsets.size() - start_row);

    for (size_t row = start_row; row != end_row; ++row)
    {
        consumer.beginRow();
        const size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = shared_data_offsets[static_cast<ssize_t>(row)];
        size_t sorted_paths_index = 0;

        for (size_t shared_index = start; shared_index != end; ++shared_index)
        {
            const auto shared_path = shared_data_paths->getDataAt(shared_index);
            while (sorted_paths_index < sorted_paths.size() && sorted_paths[sorted_paths_index].path < shared_path)
            {
                consume_path(sorted_paths[sorted_paths_index], row);
                ++sorted_paths_index;
            }

            consume_shared(shared_path, shared_data_values->getDataAt(shared_index));
        }

        for (; sorted_paths_index < sorted_paths.size(); ++sorted_paths_index)
            consume_path(sorted_paths[sorted_paths_index], row);

        consumer.endRow();
    }
}

}
