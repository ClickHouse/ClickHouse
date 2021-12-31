#include "SourceMeiliSearch.h"
#include <base/JSON.h>
#include <base/range.h>
#include "Common/Exception.h"
#include "Common/quoteString.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int MEILISEARCH_EXCEPTION;
}

MeiliSearchSource::MeiliSearchSource(
    const MeiliSearchConfiguration & config,
    const Block & sample_block,
    UInt64 max_block_size_,
    UInt64 offset_,
    std::unordered_map<String, String> query_params_)
    : SourceWithProgress(sample_block.cloneEmpty())
    , connection(config)
    , max_block_size{max_block_size_}
    , query_params{query_params_}
    , offset{offset_}
{
    description.init(sample_block);

    String columns_to_get = "[";
    for (const auto & col : description.sample_block)
    {
        columns_to_get += doubleQuoteString(col.name) + ",";
    }
    columns_to_get.back() = ']';

    query_params[doubleQuoteString("attributesToRetrieve")] = columns_to_get;
    query_params[doubleQuoteString("limit")] = std::to_string(max_block_size);
}


MeiliSearchSource::~MeiliSearchSource() = default;

void insertWithTypeId(MutableColumnPtr & column, JSON kv_pair, int type_id)
{
    if (type_id == Field::Types::UInt64)
    {
        auto value = kv_pair.getValue().get<UInt64>();
        column->insert(value);
    }
    else if (type_id == Field::Types::Int64)
    {
        auto value = kv_pair.getValue().get<Int64>();
        column->insert(value);
    }
    else if (type_id == Field::Types::String)
    {
        auto value = kv_pair.getValue().get<String>();
        column->insert(value);
    }
    else if (type_id == Field::Types::Float64)
    {
        auto value = kv_pair.getValue().get<Float64>();
        column->insert(value);
    }
}

Chunk MeiliSearchSource::generate()
{
    if (all_read)
    {
        return {};
    }

    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    for (const auto i : collections::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    query_params[doubleQuoteString("offset")] = std::to_string(offset);
    auto response = connection.searchQuery(query_params);

    JSON jres = JSON(response).begin();

    if (jres.getName() == "message")
    {
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());
    }

    size_t cnt_match = 0;
    String def;

    for (const auto json : jres.getValue())
    {
        ++cnt_match;
        for (const auto kv_pair : json)
        {
            const auto & name = kv_pair.getName();
            auto & col = columns[description.sample_block.getPositionByName(name)];
            Field::Types::Which type_id = description.sample_block.getByName(name).type->getDefault().getType();
            insertWithTypeId(col, kv_pair, type_id);
        }
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
