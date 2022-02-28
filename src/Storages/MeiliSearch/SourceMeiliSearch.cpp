#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MeiliSearch/SourceMeiliSearch.h>
#include <base/JSON.h>
#include <base/range.h>
#include <base/types.h>
#include <magic_enum.hpp>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEILISEARCH_EXCEPTION;
    extern const int UNSUPPORTED_MEILISEARCH_TYPE;
    extern const int MEILISEARCH_MISSING_SOME_COLUMNS;
}

MeiliSearchSource::MeiliSearchSource(
    const MeiliSearchConfiguration & config,
    const Block & sample_block,
    UInt64 max_block_size_,
    std::unordered_map<String, String> query_params_)
    : SourceWithProgress(sample_block.cloneEmpty())
    , connection(config)
    , max_block_size{max_block_size_}
    , query_params{query_params_}
    , offset{0}
{
    description.init(sample_block);

    String columns_to_get = "[";
    for (const auto & col : description.sample_block)
        columns_to_get += doubleQuoteString(col.name) + ",";

    columns_to_get.back() = ']';

    query_params[doubleQuoteString("attributesToRetrieve")] = columns_to_get;
    query_params[doubleQuoteString("limit")] = std::to_string(max_block_size);
}


MeiliSearchSource::~MeiliSearchSource() = default;

Field getField(JSON value, DataTypePtr type_ptr)
{
    TypeIndex type_id = type_ptr->getTypeId();

    if (type_id == TypeIndex::UInt64 || type_id == TypeIndex::UInt32 || type_id == TypeIndex::UInt16 || type_id == TypeIndex::UInt8)
    {
        if (value.isBool())
            return value.getBool();
        else
            return value.get<UInt64>();
    }
    else if (type_id == TypeIndex::Int64 || type_id == TypeIndex::Int32 || type_id == TypeIndex::Int16 || type_id == TypeIndex::Int8)
    {
        return value.get<Int64>();
    }
    else if (type_id == TypeIndex::String)
    {
        if (value.isObject())
            return value.toString();
        else
            return value.get<String>();
    }
    else if (type_id == TypeIndex::Float64 || type_id == TypeIndex::Float32)
    {
        return value.get<Float64>();
    }
    else if (type_id == TypeIndex::Date)
    {
        return UInt16{LocalDate{String(value.toString())}.getDayNum()};
    }
    else if (type_id == TypeIndex::Date32)
    {
        return Int32{LocalDate{String(value.toString())}.getExtenedDayNum()};
    }
    else if (type_id == TypeIndex::DateTime)
    {
        ReadBufferFromString in(value.toString());
        time_t time = 0;
        readDateTimeText(time, in, assert_cast<const DataTypeDateTime *>(type_ptr.get())->getTimeZone());
        if (time < 0)
            time = 0;
        return time;
    }
    else if (type_id == TypeIndex::Nullable)
    {
        if (value.isNull())
            return Null();

        const auto * null_type = typeid_cast<const DataTypeNullable *>(type_ptr.get());
        DataTypePtr nested = null_type->getNestedType();

        return getField(value, nested);
    }
    else if (type_id == TypeIndex::Array)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(type_ptr.get());
        DataTypePtr nested = array_type->getNestedType();

        Array array;
        for (const auto el : value)
            array.push_back(getField(el, nested));

        return array;
    }
    else
    {
        const std::string_view type_name = magic_enum::enum_name(type_id);
        const String err_msg = "MeiliSearch storage doesn't support type: ";
        throw Exception(ErrorCodes::UNSUPPORTED_MEILISEARCH_TYPE, err_msg + type_name.data());
    }
}

void insertWithTypeId(MutableColumnPtr & column, JSON value, DataTypePtr type_ptr)
{
    column->insert(getField(value, type_ptr));
}

Chunk MeiliSearchSource::generate()
{
    if (all_read)
        return {};

    MutableColumns columns = description.sample_block.cloneEmptyColumns();

    query_params[doubleQuoteString("offset")] = std::to_string(offset);
    auto response = connection.searchQuery(query_params);

    JSON jres = JSON(response).begin();

    if (jres.getName() == "message")
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());

    size_t cnt_match = 0;
    String def;

    for (const auto json : jres.getValue())
    {
        ++cnt_match;
        size_t cnt_fields = 0;
        for (const auto kv_pair : json)
        {
            ++cnt_fields;
            const auto & name = kv_pair.getName();
            int pos = description.sample_block.getPositionByName(name);
            MutableColumnPtr & col = columns[pos];
            DataTypePtr type_ptr = description.sample_block.getByPosition(pos).type;
            insertWithTypeId(col, kv_pair.getValue(), type_ptr);
        }
        if (cnt_fields != columns.size())
            throw Exception(
                ErrorCodes::MEILISEARCH_MISSING_SOME_COLUMNS, "Some columns were not found in the table, json = " + json.toString());
    }


    offset += cnt_match;

    if (cnt_match == 0)
    {
        all_read = true;
        return {};
    }

    return Chunk(std::move(columns), cnt_match);
}


}
