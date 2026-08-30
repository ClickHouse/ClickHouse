#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesCache.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Functions/JSONPathValues.h>
#include <Functions/JSONValueEnumerator.h>
#include <IO/WriteBuffer.h>

#include <array>

namespace DB::JSONPathValues
{

class PrefixHashWriteBuffer final : public WriteBuffer
{
public:
    PrefixHashWriteBuffer(char * scratch, size_t scratch_size, String & prefix_, size_t prefix_limit_)
        : WriteBuffer(scratch, scratch_size)
        , prefix(prefix_)
        , prefix_limit(prefix_limit_)
    {
        prefix.clear();
    }

    bool isComplete() const { return total_size <= prefix_limit; }
    ValueHash getHash() { return valueHashFromUInt64(hash.get64()); }

private:
    void nextImpl() override
    {
        const size_t size = offset();
        hash.update(working_buffer.begin(), size);
        if (prefix.size() < prefix_limit)
            prefix.append(working_buffer.begin(), std::min(size, prefix_limit - prefix.size()));
        total_size += size;
        position() = working_buffer.begin();
    }

    void finalizeImpl() override { nextImpl(); }

    String & prefix;
    size_t prefix_limit;
    size_t total_size = 0;
    SipHash hash{VALUE_HASH_KEY0, VALUE_HASH_KEY1};
};

template <typename Consumer>
class Extractor
{
public:
    Extractor(size_t max_token_bytes_, Consumer & consumer_)
        : max_token_bytes(max_token_bytes_)
        , consumer(consumer_)
    {
    }

    void consumeNull(std::string_view, bool) {}
    void setRow(size_t row) { consumer.setRow(row); }
    void finishRows(size_t rows) { consumer.finishRows(rows); }

    void consumeSharedScalar(std::string_view path, BinaryTypeIndex type_index, std::string_view value)
    {
        const auto & element = getSimpleDataTypesCache().getElement(type_index);
        preparePathType(path, *element.type, element.name);

        const size_t capacity = valueCapacity(path_type_prefix);
        const bool complete = value.size() <= capacity;
        if (complete && value.empty())
            return;

        if (encodeValueTo(
                token_buffer,
                path_type_prefix,
                value,
                max_token_bytes,
                complete,
                Kind::ScalarComplete,
                Kind::ScalarTruncated,
                complete ? ValueHash{} : hashValue(value)))
            consumer.addToken(token_buffer);
    }

    void consumeValue(
        std::string_view path,
        const IDataType & data_type,
        std::string_view type_name,
        const ISerialization & serialization,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic,
        const FormatSettings & format_settings)
    {
        preparePathType(path, data_type, type_name);

        const IColumn * value_column = &source_column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(value_column))
            value_column = &nullable->getNestedColumn();

        if (cached_array_type)
        {
            insertArray(
                path,
                path_type_prefix,
                *cached_array_type,
                serialization,
                *value_column,
                row,
                format_settings);
            if (is_dynamic)
                insertDynamicValidation(path);
            return;
        }

        if (cached_is_map)
        {
            if (cached_map_type && !is_dynamic)
                insertMap(path_type_prefix, *value_column, row);
            if (is_dynamic)
                insertDynamicValidation(path);
            return;
        }

        if (cached_base_type->getTypeId() == TypeIndex::Object)
        {
            PrefixedConsumer nested_consumer(*this, path);
            DB::enumerateJSONValues(
                assert_cast<const ColumnObject &>(*value_column),
                assert_cast<const DataTypeObject &>(*cached_base_type),
                nested_consumer,
                row,
                1);
            if (is_dynamic)
                insertDynamicValidation(path);
            return;
        }

        const size_t typed_capacity = valueCapacity(path_type_prefix);
        if (typed_capacity == 0 && path_type_prefix.size() + 1 > max_token_bytes)
        {
            if (is_dynamic && !cached_is_string && !cached_is_supported_dynamic_scalar)
                insertDynamicValidation(path);
            return;
        }

        const auto rendered = render(
            cached_is_string ? value_column : nullptr,
            source_column,
            serialization,
            row,
            typed_capacity,
            format_settings);

        const bool typed_complete = rendered.complete && rendered.value.size() <= typed_capacity;
        if (!(cached_is_string && typed_complete && rendered.value.empty()))
        {
            if (encodeValueTo(
                    token_buffer,
                    path_type_prefix,
                    rendered.value,
                    max_token_bytes,
                    typed_complete,
                    Kind::ScalarComplete,
                    Kind::ScalarTruncated,
                    rendered.hash))
                consumer.addToken(token_buffer);
        }

        if (is_dynamic && !cached_is_string && !cached_is_supported_dynamic_scalar)
            insertDynamicValidation(path);
    }

private:
    class PrefixedConsumer
    {
    public:
        PrefixedConsumer(Extractor & extractor_, std::string_view prefix_)
            : extractor(extractor_)
            , prefix(prefix_)
        {
        }

        void consumeSharedScalar(std::string_view path, BinaryTypeIndex type_index, std::string_view value)
        {
            extractor.consumeSharedScalar(prefixed(path), type_index, value);
        }

        void consumeNull(std::string_view path, bool is_typed_nullable)
        {
            extractor.consumeNull(prefixed(path), is_typed_nullable);
        }

        void consumeValue(
            std::string_view path,
            const IDataType & data_type,
            std::string_view type_name,
            const ISerialization & serialization,
            const IColumn & source_column,
            size_t row,
            bool is_dynamic,
            const FormatSettings & format_settings)
        {
            extractor.consumeValue(
                prefixed(path),
                data_type,
                type_name,
                serialization,
                source_column,
                row,
                is_dynamic,
                format_settings);
        }

        void setRow(size_t) {}
        void finishRows(size_t) {}

    private:
        String prefixed(std::string_view path) const
        {
            String result = prefix;
            result += '.';
            result += path;
            return result;
        }

        Extractor & extractor;
        String prefix;
    };

    class ArrayJSONConsumer
    {
    public:
        ArrayJSONConsumer(Extractor & extractor_, std::string_view prefix_)
            : extractor(extractor_)
            , prefix(prefix_)
        {
        }

        void consumeSharedScalar(std::string_view path, BinaryTypeIndex type_index, std::string_view value)
        {
            const String full_path = prefixed(path);
            const auto & element = getSimpleDataTypesCache().getElement(type_index);
            extractor.insertArrayJSONSharedScalar(full_path, *element.type, element.name, value);
        }

        void consumeNull(std::string_view, bool) {}

        void consumeValue(
            std::string_view path,
            const IDataType & data_type,
            std::string_view type_name,
            const ISerialization & serialization,
            const IColumn & source_column,
            size_t row,
            bool,
            const FormatSettings & format_settings)
        {
            const String full_path = prefixed(path);
            const auto base_type = removeLowCardinality(removeNullableOrLowCardinalityNullable(data_type.getPtr()));
            if (base_type->getTypeId() == TypeIndex::Object)
            {
                const IColumn * value_column = &source_column;
                if (const auto * nullable = typeid_cast<const ColumnNullable *>(value_column))
                    value_column = &nullable->getNestedColumn();
                ArrayJSONConsumer nested_consumer(extractor, full_path);
                DB::enumerateJSONValues(
                    assert_cast<const ColumnObject &>(*value_column),
                    assert_cast<const DataTypeObject &>(*base_type),
                    nested_consumer,
                    row,
                    1);
                return;
            }

            extractor.insertArrayJSONLeaf(
                full_path, data_type, type_name, serialization, source_column, row, format_settings);
        }

        void setRow(size_t) {}
        void finishRows(size_t) {}

    private:
        String prefixed(std::string_view path) const
        {
            String result = prefix;
            result += '.';
            result += path;
            return result;
        }

        Extractor & extractor;
        String prefix;
    };

    void preparePathType(std::string_view path, const IDataType & data_type, std::string_view type_name)
    {
        const bool type_changed = !cached_binary_type || type_name != cached_type_name;
        if (type_changed)
        {
            cached_type_name.assign(type_name);
            cached_type = removeNullableOrLowCardinalityNullable(data_type.getPtr());
            cached_base_type = removeLowCardinality(cached_type);
            cached_array_type = typeid_cast<const DataTypeArray *>(cached_type.get());
            cached_map_type = typeid_cast<const DataTypeMap *>(cached_type.get());
            cached_is_map = cached_map_type;
            if (cached_map_type
                && (!WhichDataType(removeLowCardinality(cached_map_type->getKeyType())).isString()
                    || !WhichDataType(removeLowCardinality(cached_map_type->getValueType())).isString()))
                cached_map_type = nullptr;
            cached_is_string = WhichDataType(cached_base_type).isStringOrFixedString();
            cached_is_supported_dynamic_scalar = isSupportedDynamicScalar(*cached_base_type);
            auto type_it = binary_types.find(cached_type_name);
            if (type_it == binary_types.end())
                type_it = binary_types.emplace(cached_type_name, encodeDataType(cached_type)).first;
            cached_binary_type = &type_it->second;
        }
        if (type_changed || path != cached_path)
        {
            cached_path.assign(path);
            encodePathTypePrefix(path_type_prefix, path, *cached_binary_type);
        }
    }
    struct RenderedValue
    {
        std::string_view value;
        bool complete = false;
        ValueHash hash{};
    };

    static bool isSupportedDynamicScalar(const IDataType & type)
    {
        const auto type_id = type.getTypeId();
        return isBool(type.getPtr())
            || type_id == TypeIndex::Int64
            || type_id == TypeIndex::UInt64
            || type_id == TypeIndex::Float64;
    }

    size_t valueCapacity(std::string_view prefix) const
    {
        if (prefix.size() + 1 > max_token_bytes)
            return 0;
        return max_token_bytes - prefix.size() - 1;
    }

    RenderedValue render(
        const IColumn * text_column,
        const IColumn & column,
        const ISerialization & serialization,
        size_t row,
        size_t prefix_limit,
        const FormatSettings & format_settings)
    {
        if (text_column)
        {
            const std::string_view value = text_column->getDataAt(row);
            const bool complete = value.size() <= prefix_limit;
            return {value, complete, complete ? ValueHash{} : hashValue(value)};
        }

        PrefixHashWriteBuffer buffer(scratch.data(), scratch.size(), serialization_prefix, prefix_limit);
        serialization.serializeText(column, row, buffer, format_settings);
        buffer.finalize();
        return {serialization_prefix, buffer.isComplete(), buffer.getHash()};
    }

    void insertArray(
        std::string_view path,
        std::string_view prefix,
        const DataTypeArray & array_type,
        const ISerialization & serialization,
        const IColumn & value_column,
        size_t row,
        const FormatSettings & format_settings)
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(&value_column);
        const ISerialization * array_value_serialization = &serialization;
        if (const auto * nullable_serialization = typeid_cast<const SerializationNullable *>(array_value_serialization))
            array_value_serialization = nullable_serialization->getNested().get();
        const auto * array_serialization = typeid_cast<const SerializationArray *>(array_value_serialization);
        if (!array_column || !array_serialization)
            return;

        const auto & nested_column = array_column->getData();
        const auto & nested_serialization = *array_serialization->getNestedSerialization();
        const auto nested_type = removeLowCardinality(
            removeNullableOrLowCardinalityNullable(array_type.getNestedType()));
        const size_t begin = array_column->getOffsets()[static_cast<ssize_t>(row) - 1];
        const size_t end = array_column->getOffsets()[row];
        if (nested_type->getTypeId() == TypeIndex::Object)
        {
            String array_path(path);
            array_path += "[]";
            ArrayJSONConsumer nested_consumer(*this, array_path);
            if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(&nested_column))
            {
                const auto & object_column = assert_cast<const ColumnObject &>(nullable_column->getNestedColumn());
                for (size_t element = begin; element != end; ++element)
                {
                    if (!nullable_column->isNullAt(element))
                        DB::enumerateJSONValues(
                            object_column,
                            assert_cast<const DataTypeObject &>(*nested_type),
                            nested_consumer,
                            element,
                            1);
                }
            }
            else
            {
                DB::enumerateJSONValues(
                    assert_cast<const ColumnObject &>(nested_column),
                    assert_cast<const DataTypeObject &>(*nested_type),
                    nested_consumer,
                    begin,
                    end - begin);
            }
            return;
        }

        if (array_type.getNestedType()->hasDynamicStructure())
            return;

        const bool nested_is_string
            = WhichDataType(removeLowCardinalityAndNullable(array_type.getNestedType())).isStringOrFixedString();
        for (size_t element = begin; element != end; ++element)
            insertArrayElement(prefix, nested_serialization, nested_column, element, nested_is_string, format_settings);
    }

    void insertArrayJSONSharedScalar(
        std::string_view path,
        const IDataType & data_type,
        std::string_view type_name,
        std::string_view value)
    {
        const auto element_type = removeNullableOrLowCardinalityNullable(data_type.getPtr());
        const auto array_type = std::make_shared<DataTypeArray>(element_type);
        const String array_type_name = "Array(" + String(type_name) + ")";
        preparePathType(path, *array_type, array_type_name);

        const size_t capacity = valueCapacity(path_type_prefix);
        const bool complete = value.size() <= capacity;
        if (complete && value.empty())
            return;

        if (encodeValueTo(
                token_buffer,
                path_type_prefix,
                value,
                max_token_bytes,
                complete,
                Kind::ScalarComplete,
                Kind::ScalarTruncated,
                complete ? ValueHash{} : hashValue(value)))
            consumer.addToken(token_buffer);
    }

    void insertArrayJSONLeaf(
        std::string_view path,
        const IDataType & data_type,
        std::string_view,
        const ISerialization & serialization,
        const IColumn & column,
        size_t row,
        const FormatSettings & format_settings)
    {
        const auto element_type = removeNullableOrLowCardinalityNullable(data_type.getPtr());
        const auto base_type = removeLowCardinality(element_type);
        const WhichDataType which(*base_type);
        if (which.isArray() || which.isMap() || which.isTuple() || which.isObject() || base_type->hasDynamicStructure())
            return;

        const auto array_type = std::make_shared<DataTypeArray>(element_type);
        preparePathType(path, *array_type, array_type->getName());
        insertArrayElement(
            path_type_prefix,
            serialization,
            column,
            row,
            which.isStringOrFixedString(),
            format_settings,
            Kind::ScalarComplete,
            Kind::ScalarTruncated);
    }

    void insertArrayElement(
        std::string_view prefix,
        const ISerialization & serialization,
        const IColumn & column,
        size_t row,
        bool is_string,
        const FormatSettings & format_settings,
        Kind complete_kind = Kind::ArrayElementComplete,
        Kind truncated_kind = Kind::ArrayElementTruncated)
    {
        if (column.isNullAt(row))
            return;

        const size_t capacity = valueCapacity(prefix);
        const IColumn * text_column = is_string ? &column : nullptr;
        const auto rendered = render(text_column, column, serialization, row, capacity, format_settings);
        if (rendered.complete && rendered.value.empty())
            return;

        if (encodeValueTo(
                token_buffer,
                prefix,
                rendered.value,
                max_token_bytes,
                rendered.complete,
                complete_kind,
                truncated_kind,
                rendered.hash))
            consumer.addToken(token_buffer);
    }

    void insertMap(std::string_view prefix, const IColumn & column, size_t row)
    {
        const auto * map_column = typeid_cast<const ColumnMap *>(&column);
        if (!map_column)
            return;

        const auto & nested = map_column->getNestedColumn();
        const auto & tuple = map_column->getNestedData();
        const auto & keys = tuple.getColumn(0);
        const auto & values = tuple.getColumn(1);
        const size_t begin = nested.getOffsets()[static_cast<ssize_t>(row) - 1];
        const size_t end = nested.getOffsets()[row];
        seen_map_keys.clear();
        for (size_t element = begin; element != end; ++element)
        {
            String key(keys.getDataAt(element));
            if (!seen_map_keys.emplace(key).second)
                continue;
            if (encodeMapEntryTo(
                    token_buffer,
                    prefix,
                    key,
                    values.getDataAt(element),
                    max_token_bytes))
                consumer.addToken(token_buffer);
        }
    }

    void insertDynamicValidation(std::string_view path)
    {
        auto token = encodeDynamicValidation(path, max_token_bytes);
        if (token)
            consumer.addToken(*token);
    }

    size_t max_token_bytes;
    std::array<char, 2048> scratch{};
    String serialization_prefix;
    String path_type_prefix;
    String token_buffer;
    String cached_path;
    String cached_type_name;
    DataTypePtr cached_type;
    DataTypePtr cached_base_type;
    const DataTypeArray * cached_array_type = nullptr;
    const DataTypeMap * cached_map_type = nullptr;
    bool cached_is_string = false;
    bool cached_is_map = false;
    bool cached_is_supported_dynamic_scalar = false;
    const String * cached_binary_type = nullptr;
    UnorderedMapWithMemoryTracking<String, String> binary_types;
    UnorderedSetWithMemoryTracking<String> seen_map_keys;
    Consumer & consumer;
};

}
