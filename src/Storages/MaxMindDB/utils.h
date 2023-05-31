#include "config.h"

#if USE_MAXMINDDB
#    include <vector>
#    include <maxminddb.h>
#    include <Core/ColumnWithTypeAndName.h>
#    include <Core/Field.h>
#    include <Storages/KVStorageUtils.h>
#    include <base/types.h>
#    include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

inline std::vector<std::string>
serializeKeysToString(FieldVector::const_iterator & it_, FieldVector::const_iterator end_, size_t max_block_size_)
{
    size_t num_keys = end_ - it_;
    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it_ < end_ && (max_block_size_ == 0 || rows_processed < max_block_size_))
    {
        const auto & field = *it_;
        auto type = field.getType();
        if (type != Field::Types::IPv4 && type != Field::Types::IPv6 && type != Field::Types::String)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected key to be String, IPv4 or IPv6 type but {} give", fieldTypeToString(type));

        result.emplace_back(toString(*it_));
        ++it_;
        ++rows_processed;
    }

    return result;
}

inline std::vector<std::string> serializeKeysToString(const ColumnWithTypeAndName & keys)
{
    if (!keys.column)
        return {};

    auto type_without_lc = removeLowCardinality(keys.type);
    if (!isString(type_without_lc) && !isIPv4(type_without_lc) && !isIPv6(type_without_lc))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected key to be String, IPv4 or IPv6 type but {} give", keys.type->getName());

    size_t num_keys = keys.column->size();
    std::vector<std::string> result;
    result.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
        result.emplace_back(toString((*keys.column)[i]));
    return result;
}


inline void fillKeyAndValueColumns(const String & key, const String & value, size_t key_pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString key_buffer(key);
    for (size_t i = 0; i < header.columns(); ++i)
    {
        if (i == key_pos)
        {
            const auto & serialization = header.getByPosition(i).type->getDefaultSerialization();
            serialization->deserializeTextEscaped(*columns[i], key_buffer, {});
        }
        else
        {
            columns[i]->insert(value);
        }
    }
}

inline String toHexString(const UInt8 * buf, size_t length)
{
    String res(length * 2, 0);
    char * out = res.data();
    for (size_t i = 0; i < length; ++i)
    {
        writeHexByteUppercase(buf[i], out);
        out += 2;
    }
    return res;
}


inline std::pair<JSONBuilder::ItemPtr, MMDB_entry_data_list_s *> dumpMMDBEntryDataList(MMDB_entry_data_list_s * entry_data_list)
{
    switch (entry_data_list->entry_data.type)
    {
        case MMDB_DATA_TYPE_MAP: {
            UInt32 size = entry_data_list->entry_data.data_size;
            auto map = std::make_unique<JSONBuilder::JSONMap>();
            for (entry_data_list = entry_data_list->next; size && entry_data_list; size--)
            {
                if (MMDB_DATA_TYPE_UTF8_STRING != entry_data_list->entry_data.type)
                    return {};

                /// Dump key
                char * key = strndup(entry_data_list->entry_data.utf8_string, entry_data_list->entry_data.data_size);
                if (nullptr == key)
                    return {};
                entry_data_list = entry_data_list->next;

                /// Dump value
                auto value_res = dumpMMDBEntryDataList(entry_data_list);
                if (!value_res.first)
                    return {};
                entry_data_list = value_res.second;

                map->add(std::string(key), std::move(value_res.first));
                free(key);
            }
            return {std::move(map), entry_data_list};
        }
        case MMDB_DATA_TYPE_ARRAY: {
            UInt32 size = entry_data_list->entry_data.data_size;
            auto array = std::make_unique<JSONBuilder::JSONArray>();
            for (entry_data_list = entry_data_list->next; size && entry_data_list; size--)
            {
                auto elem_res = dumpMMDBEntryDataList(entry_data_list);
                if (!elem_res.first)
                    return {};
                entry_data_list = elem_res.second;

                array->add(std::move(elem_res.first));
            }
            return {std::move(array), entry_data_list};
        }
        case MMDB_DATA_TYPE_UTF8_STRING: {
            char * str = strndup(entry_data_list->entry_data.utf8_string, entry_data_list->entry_data.data_size);
            if (nullptr == str)
                return {};
            entry_data_list = entry_data_list->next;

            auto json_str = std::make_unique<JSONBuilder::JSONString>(std::string_view(str));
            free(str);
            return {std::move(json_str), entry_data_list};
        }
        case MMDB_DATA_TYPE_BYTES: {
            auto hex_str
                = toHexString(reinterpret_cast<const UInt8 *>(entry_data_list->entry_data.bytes), entry_data_list->entry_data.data_size);
            entry_data_list = entry_data_list->next;

            auto json_str = std::make_unique<JSONBuilder::JSONString>(std::move(hex_str));
            return {std::move(json_str), entry_data_list};
        }
        case MMDB_DATA_TYPE_DOUBLE: {
            auto value = entry_data_list->entry_data.double_value;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<Float64>>(value);
            return {std::move(json_num), entry_data_list};
        }
        case MMDB_DATA_TYPE_FLOAT: {
            auto value = entry_data_list->entry_data.float_value;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<Float32>>(value);
            return {std::move(json_num), entry_data_list};
        }
        case MMDB_DATA_TYPE_UINT16: {
            auto value = entry_data_list->entry_data.uint16;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<UInt16>>(value);
            return {std::move(json_num), entry_data_list};
        }
        case MMDB_DATA_TYPE_UINT32: {
            auto value = entry_data_list->entry_data.uint32;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<UInt32>>(value);
            return {std::move(json_num), entry_data_list};
        }
        case MMDB_DATA_TYPE_BOOLEAN: {
            auto value = entry_data_list->entry_data.boolean;
            entry_data_list = entry_data_list->next;

            auto json_bool = std::make_unique<JSONBuilder::JSONBool>(value);
            return {std::move(json_bool), entry_data_list};
        }
        case MMDB_DATA_TYPE_UINT64: {
            auto value = entry_data_list->entry_data.uint64;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<UInt64>>(value);
            return {std::move(json_num), entry_data_list};
        }
        case MMDB_DATA_TYPE_UINT128: {
            auto hex_str = toHexString(reinterpret_cast<const UInt8 *>(&entry_data_list->entry_data.uint128), 16);
            auto json = std::make_unique<JSONBuilder::JSONString>(std::move(hex_str));

            entry_data_list = entry_data_list->next;
            return {std::move(json), entry_data_list};
        }
        case MMDB_DATA_TYPE_INT32: {
            auto value = entry_data_list->entry_data.int32;
            entry_data_list = entry_data_list->next;

            auto json_num = std::make_unique<JSONBuilder::JSONNumber<Int32>>(value);
            return {std::move(json_num), entry_data_list};
        }
        default:
            return {};
    }
}

}

#endif
