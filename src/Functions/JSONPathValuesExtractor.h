#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/StringHashForHeterogeneousLookup.h>
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
    struct PreparedType
    {
        DataTypePtr type;
        DataTypePtr base_type;
        String prefix;
        mutable String array_prefix;
        const DataTypeArray * array_type = nullptr;
        const DataTypeMap * map_type = nullptr;
        bool is_string = false;
        bool is_map = false;
        bool is_supported_dynamic_scalar = false;
    };

public:
    struct PreparedPath
    {
        String path;
        String filter_path;
        const IDataType * static_type = nullptr;
        bool should_visit = false;
        bool should_index = false;
        bool has_json_descendants = false;
        mutable UnorderedMapWithMemoryTracking<String, PreparedType, StringHashForHeterogeneousLookup, std::equal_to<>> types;
        mutable const std::pair<const String, PreparedType> * last_type = nullptr;
        mutable UnorderedMapWithMemoryTracking<String, PreparedPath *, StringHashForHeterogeneousLookup, std::equal_to<>> children;
        mutable UnorderedMapWithMemoryTracking<String, PreparedPath *, StringHashForHeterogeneousLookup, std::equal_to<>> array_children;
    };

    Extractor(size_t max_token_bytes_, const PathMatcher & path_matcher_, Consumer & consumer_)
        : max_token_bytes(max_token_bytes_)
        , path_matcher(path_matcher_)
        , consumer(consumer_)
    {
    }

    void consumeNull(std::string_view, bool) {}
    const PreparedPath * preparePath(std::string_view path, const IDataType * static_type = nullptr)
    {
        auto it = literal_path_cache.find(path);
        if (it == literal_path_cache.end())
        {
            it = literal_path_cache.try_emplace(String(path)).first;
            it->second.path = it->first;
            it->second.filter_path = escapeLiteralPath(path);
            preparePathFilter(it->second);
        }
        prepareStaticType(it->second, static_type);
        return it->second.should_visit ? &it->second : nullptr;
    }
    bool shouldConsumeValue(const PreparedPath & path, const IDataType & data_type) const
    {
        return path.should_index || (path.static_type == &data_type ? path.has_json_descendants : hasJSONDescendants(data_type));
    }
    void setRow(size_t row) { consumer.setRow(row); }
    void finishRows(size_t rows) { consumer.finishRows(rows); }
    JSONValueEnumerationState & getEnumerationState() { return enumeration_state; }

    void consumeSharedScalar(const PreparedPath & path, BinaryTypeIndex type_index, std::string_view value)
    {
        const auto & element = getSimpleDataTypesCache().getElement(type_index);
        const auto & prepared = preparePathType(path, *element.type, element.name);
        const auto & path_type_prefix = prepared.prefix;

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
        const PreparedPath & path,
        const IDataType & data_type,
        std::string_view type_name,
        const ISerialization & serialization,
        const IColumn & source_column,
        size_t row,
        bool is_dynamic,
        const FormatSettings & format_settings)
    {
        const auto & prepared = preparePathType(path, data_type, type_name);
        const auto & path_type_prefix = prepared.prefix;

        const IColumn * value_column = &source_column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(value_column))
            value_column = &nullable->getNestedColumn();

        if (prepared.array_type)
        {
            insertArray(
                path, path_type_prefix, *prepared.array_type, serialization, *value_column, row, format_settings);
            if (is_dynamic && path.should_index)
                insertDynamicValidation(path.path);
            return;
        }

        if (prepared.is_map)
        {
            if (path.should_index && prepared.map_type && !is_dynamic)
                insertMap(path_type_prefix, *value_column, row);
            if (is_dynamic && path.should_index)
                insertDynamicValidation(path.path);
            return;
        }

        if (prepared.base_type->getTypeId() == TypeIndex::Object)
        {
            PrefixedConsumer nested_consumer(*this, path);
            DB::enumerateJSONValues(
                assert_cast<const ColumnObject &>(*value_column),
                assert_cast<const DataTypeObject &>(*prepared.base_type),
                nested_consumer,
                row,
                1);
            if (is_dynamic && path.should_index)
                insertDynamicValidation(path.path);
            return;
        }

        if (!path.should_index)
            return;

        const size_t typed_capacity = valueCapacity(path_type_prefix);
        if (typed_capacity == 0 && path_type_prefix.size() + 1 > max_token_bytes)
        {
            if (is_dynamic && !prepared.is_string && !prepared.is_supported_dynamic_scalar)
                insertDynamicValidation(path.path);
            return;
        }

        const auto rendered = render(
            prepared.is_string ? value_column : nullptr,
            source_column,
            serialization,
            row,
            typed_capacity,
            format_settings);

        const bool typed_complete = rendered.complete && rendered.value.size() <= typed_capacity;
        if (!(prepared.is_string && typed_complete && rendered.value.empty()))
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

        if (is_dynamic && !prepared.is_string && !prepared.is_supported_dynamic_scalar)
            insertDynamicValidation(path.path);
    }

private:
    static bool hasJSONDescendants(const IDataType & data_type)
    {
        const auto unwrapped_type = removeLowCardinality(removeNullableOrLowCardinalityNullable(data_type.getPtr()));
        const auto * unwrapped_array_type = typeid_cast<const DataTypeArray *>(unwrapped_type.get());
        return unwrapped_type->getTypeId() == TypeIndex::Object
            || (unwrapped_array_type
                && removeLowCardinality(removeNullableOrLowCardinalityNullable(unwrapped_array_type->getNestedType()))->getTypeId()
                    == TypeIndex::Object);
    }

    void preparePathFilter(PreparedPath & path)
    {
        path.should_visit = path_matcher.shouldVisit(path.filter_path);
        path.should_index = path.should_visit && path_matcher.shouldIndex(path.filter_path);
    }

    static void prepareStaticType(PreparedPath & path, const IDataType * static_type)
    {
        if (!static_type || path.static_type == static_type)
            return;
        path.static_type = static_type;
        path.has_json_descendants = hasJSONDescendants(*static_type);
    }

    const PreparedPath * preparePrefixedPath(
        const PreparedPath & parent, std::string_view path, const IDataType * static_type, bool array_elements)
    {
        auto & children = array_elements ? parent.array_children : parent.children;
        auto child_it = children.find(path);
        if (child_it == children.end())
        {
            String full_path = parent.path;
            String filter_path = parent.filter_path;
            if (array_elements)
            {
                full_path += "[]";
                filter_path += "[]";
            }
            full_path += '.';
            full_path += path;
            filter_path += '.';
            filter_path += escapeLiteralPath(path);
            auto [it, inserted] = prefixed_path_cache.try_emplace(filter_path);
            if (inserted)
            {
                it->second.path = std::move(full_path);
                it->second.filter_path = std::move(filter_path);
                preparePathFilter(it->second);
            }
            child_it = children.emplace(String(path), &it->second).first;
        }
        auto & prepared = *child_it->second;
        prepareStaticType(prepared, static_type);
        return prepared.should_visit ? &prepared : nullptr;
    }

    class PrefixedConsumer
    {
    public:
        using PreparedPath = Extractor::PreparedPath;

        PrefixedConsumer(Extractor & extractor_, const PreparedPath & parent_)
            : extractor(extractor_)
            , parent(parent_)
        {
        }

        const PreparedPath * preparePath(std::string_view path, const IDataType * static_type = nullptr)
        {
            return extractor.preparePrefixedPath(parent, path, static_type, false);
        }

        bool shouldConsumeValue(const PreparedPath & path, const IDataType & data_type) const
        {
            return extractor.shouldConsumeValue(path, data_type);
        }

        void consumeSharedScalar(const PreparedPath & path, BinaryTypeIndex type_index, std::string_view value)
        {
            extractor.consumeSharedScalar(path, type_index, value);
        }

        void consumeNull(std::string_view, bool) { }

        void consumeValue(
            const PreparedPath & path,
            const IDataType & data_type,
            std::string_view type_name,
            const ISerialization & serialization,
            const IColumn & source_column,
            size_t row,
            bool is_dynamic,
            const FormatSettings & format_settings)
        {
            extractor.consumeValue(path, data_type, type_name, serialization, source_column, row, is_dynamic, format_settings);
        }

        void setRow(size_t) {}
        void finishRows(size_t) {}
        JSONValueEnumerationState & getEnumerationState() { return extractor.getEnumerationState(); }

    private:
        Extractor & extractor;
        const PreparedPath & parent;
    };

    class ArrayJSONConsumer
    {
    public:
        using PreparedPath = Extractor::PreparedPath;

        ArrayJSONConsumer(Extractor & extractor_, const PreparedPath & parent_, bool array_elements_)
            : extractor(extractor_)
            , parent(parent_)
            , array_elements(array_elements_)
        {
        }

        const PreparedPath * preparePath(std::string_view path, const IDataType * static_type = nullptr)
        {
            return extractor.preparePrefixedPath(parent, path, static_type, array_elements);
        }

        bool shouldConsumeValue(const PreparedPath & path, const IDataType & data_type) const
        {
            return extractor.shouldConsumeValue(path, data_type);
        }

        void consumeSharedScalar(const PreparedPath & path, BinaryTypeIndex type_index, std::string_view value)
        {
            const auto & element = getSimpleDataTypesCache().getElement(type_index);
            extractor.insertArrayJSONSharedScalar(path, *element.type, element.name, value);
        }

        void consumeNull(std::string_view, bool) {}

        void consumeValue(
            const PreparedPath & path,
            const IDataType & data_type,
            std::string_view type_name,
            const ISerialization & serialization,
            const IColumn & source_column,
            size_t row,
            bool,
            const FormatSettings & format_settings)
        {
            const auto base_type = removeLowCardinality(removeNullableOrLowCardinalityNullable(data_type.getPtr()));
            if (base_type->getTypeId() == TypeIndex::Object)
            {
                const IColumn * value_column = &source_column;
                if (const auto * nullable = typeid_cast<const ColumnNullable *>(value_column))
                    value_column = &nullable->getNestedColumn();
                ArrayJSONConsumer nested_consumer(extractor, path, false);
                DB::enumerateJSONValues(
                    assert_cast<const ColumnObject &>(*value_column),
                    assert_cast<const DataTypeObject &>(*base_type),
                    nested_consumer,
                    row,
                    1);
                return;
            }

            extractor.insertArrayJSONLeaf(path, data_type, type_name, serialization, source_column, row, format_settings);
        }

        void setRow(size_t) {}
        void finishRows(size_t) {}
        JSONValueEnumerationState & getEnumerationState() { return extractor.getEnumerationState(); }

    private:
        Extractor & extractor;
        const PreparedPath & parent;
        bool array_elements;
    };

    const PreparedType & preparePathType(const PreparedPath & path, const IDataType & data_type, std::string_view type_name)
    {
        if (path.last_type && path.last_type->first == type_name)
            return path.last_type->second;
        auto it = path.types.find(type_name);
        if (it == path.types.end())
        {
            it = path.types.try_emplace(String(type_name)).first;
            auto & prepared = it->second;
            prepared.type = removeNullableOrLowCardinalityNullable(data_type.getPtr());
            prepared.base_type = removeLowCardinality(prepared.type);
            prepared.array_type = typeid_cast<const DataTypeArray *>(prepared.type.get());
            prepared.map_type = typeid_cast<const DataTypeMap *>(prepared.type.get());
            prepared.is_map = prepared.map_type;
            if (prepared.map_type
                && (!WhichDataType(removeLowCardinality(prepared.map_type->getKeyType())).isString()
                    || !WhichDataType(removeLowCardinality(prepared.map_type->getValueType())).isString()))
                prepared.map_type = nullptr;
            prepared.is_string = WhichDataType(prepared.base_type).isStringOrFixedString();
            prepared.is_supported_dynamic_scalar = isSupportedDynamicScalar(*prepared.base_type);
            auto type_it = binary_types.find(it->first);
            if (type_it == binary_types.end())
                type_it = binary_types.emplace(it->first, encodeDataType(prepared.type)).first;
            encodePathTypePrefix(prepared.prefix, path.path, type_it->second);
        }
        path.last_type = &*it;
        return it->second;
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
        const PreparedPath & path,
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
            ArrayJSONConsumer nested_consumer(*this, path, true);
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
        const PreparedPath & path,
        const IDataType & data_type,
        std::string_view type_name,
        std::string_view value)
    {
        const auto & prepared = preparePathType(path, data_type, type_name);
        const auto & path_type_prefix = prepareArrayPrefix(path, prepared);

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
        const PreparedPath & path,
        const IDataType & data_type,
        std::string_view type_name,
        const ISerialization & serialization,
        const IColumn & column,
        size_t row,
        const FormatSettings & format_settings)
    {
        const auto & prepared = preparePathType(path, data_type, type_name);
        const WhichDataType which(*prepared.base_type);
        if (which.isArray() || which.isMap() || which.isTuple() || which.isObject() || prepared.base_type->hasDynamicStructure())
            return;

        const auto & path_type_prefix = prepareArrayPrefix(path, prepared);
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

    static const String & prepareArrayPrefix(const PreparedPath & path, const PreparedType & type)
    {
        if (type.array_prefix.empty())
            type.array_prefix = encodePathTypePrefix(path.path, std::make_shared<DataTypeArray>(type.type));
        return type.array_prefix;
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
    JSONValueEnumerationState enumeration_state;
    const PathMatcher & path_matcher;
    std::array<char, 2048> scratch{};
    String serialization_prefix;
    String token_buffer;
    UnorderedMapWithMemoryTracking<String, PreparedPath, StringHashForHeterogeneousLookup, std::equal_to<>> literal_path_cache;
    UnorderedMapWithMemoryTracking<String, PreparedPath> prefixed_path_cache;
    UnorderedMapWithMemoryTracking<String, String> binary_types;
    UnorderedSetWithMemoryTracking<String> seen_map_keys;
    Consumer & consumer;
};

}
