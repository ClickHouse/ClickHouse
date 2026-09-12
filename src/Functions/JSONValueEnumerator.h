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

struct JSONValueEnumerationState
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

    struct ObjectPlan
    {
        ColumnPtr column;
        DataTypePtr type;
        VectorWithMemoryTracking<PathInfo> paths;
    };

    UnorderedMapWithMemoryTracking<const ColumnObject *, ObjectPlan> object_plans;
    UnorderedMapWithMemoryTracking<String, SerializationPtr> serializations;
    UnorderedMapWithMemoryTracking<String, VectorWithMemoryTracking<MutableColumnPtr>> shared_columns;
    size_t shared_value_depth = 0;
    const FormatSettings format_settings;
};

template <typename Consumer>
void enumerateJSONValues(
    const ColumnObject & column_object,
    const DataTypeObject & type_object,
    Consumer & consumer,
    size_t start_row = 0,
    size_t num_rows = std::numeric_limits<size_t>::max())
{
    using PathInfo = JSONValueEnumerationState::PathInfo;
    auto & state = consumer.getEnumerationState();
    auto prepare_paths = [&]
    {
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
        return sorted_paths;
    };

    JSONValueEnumerationState::ObjectPlan temporary_plan;
    /// Deserialized shared columns can change their paths and dynamic variants between values.
    auto & plan = state.shared_value_depth ? temporary_plan : state.object_plans[&column_object];
    if (plan.type.get() != &type_object)
    {
        plan.column = column_object.getPtr();
        plan.type = type_object.getPtr();
        plan.paths = prepare_paths();
    }
    auto & serializations_cache = state.serializations;

    const auto & shared_data_offsets = column_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
    const auto & format_settings = state.format_settings;
    using PreparedPath = typename Consumer::PreparedPath;

    auto consume_shared = [&](std::string_view path, std::string_view value_data, const PreparedPath * prepared_path = nullptr)
    {
        if (!prepared_path)
            prepared_path = consumer.preparePath(path);
        if (!prepared_path)
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

        if (has_cached_type && !consumer.shouldConsumeValue(*prepared_path, *cache.getElement(binary_type_index).type))
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
            consumer.consumeSharedScalar(*prepared_path, binary_type_index, std::string_view(buffer.position(), size));
            return;
        }
        if (binary_type_index == BinaryTypeIndex::Bool)
        {
            ++buffer.position();
            UInt8 value = 0;
            readBinary(value, buffer);
            consumer.consumeSharedScalar(
                *prepared_path,
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
            consumer.consumeSharedScalar(*prepared_path, binary_type_index, std::string_view(text, end - text));
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
        if (!has_cached_type && !consumer.shouldConsumeValue(*prepared_path, *type))
            return;

        auto & available_columns = state.shared_columns[*type_name];
        auto column = available_columns.empty() ? type->createColumn() : std::move(available_columns.back());
        if (!available_columns.empty())
            available_columns.pop_back();
        serialization->deserializeBinary(*column, buffer, format_settings);
        ++state.shared_value_depth;
        consumer.consumeValue(*prepared_path, *type, *type_name, *serialization, *column, 0, true, format_settings);
        --state.shared_value_depth;
        column->popBack(1);
        available_columns.push_back(std::move(column));
    };

    auto consume_path = [&](PathInfo & entry, const auto & prepared_path, size_t row)
    {
        if ((entry.is_dynamic || entry.is_nullable) && entry.column->isNullAt(row))
        {
            consumer.consumeNull(entry.path, entry.is_nullable);
            return;
        }

        if (!entry.is_dynamic)
        {
            consumer.consumeValue(
                prepared_path, *entry.type, entry.type_name, *entry.serialization, *entry.column, row, false, format_settings);
            return;
        }

        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*entry.column);
        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        const size_t variant_row = variant_column.offsetAt(row);

        if (discriminator == dynamic_column.getSharedVariantDiscriminator())
        {
            consume_shared(entry.path, dynamic_column.getSharedVariant().getDataAt(variant_row), &prepared_path);
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

        if (!consumer.shouldConsumeValue(prepared_path, *entry.cached_dynamic_type))
            return;

        consumer.consumeValue(
            prepared_path,
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

    for (auto & path : plan.paths)
    {
        const auto * prepared_path = consumer.preparePath(path.path, path.is_dynamic ? nullptr : path.type.get());
        if (!prepared_path)
            continue;
        if (!path.is_dynamic && !consumer.shouldConsumeValue(*prepared_path, *path.type))
            continue;

        for (size_t row = start_row; row != end_row; ++row)
        {
            consumer.setRow(row - start_row);
            consume_path(path, *prepared_path, row);
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
