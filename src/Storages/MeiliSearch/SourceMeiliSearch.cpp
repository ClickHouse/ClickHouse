#include "SourceMeiliSearch.h"
#include <base/range.h>
#include <base/JSON.h>
#include "Common/Exception.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int MEILISEARCH_EXCEPTION;
}

MeiliSearchSource::MeiliSearchSource(
    const MeiliSearchConfiguration& config,
    const Block & sample_block,
    UInt64 max_block_size_,
    UInt64 offset_)
    : SourceWithProgress(sample_block.cloneEmpty()) 
    , connection(config)
    , max_block_size{max_block_size_}
    , offset{offset_}
{
    description.init(sample_block);
}


MeiliSearchSource::~MeiliSearchSource() = default;

String quotify(const String& s) {
    return "\"" + s + "\"";
}

Chunk MeiliSearchSource::generate()
{
    if (all_read) {
        return {};
    }

    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    for (const auto i : collections::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    std::vector<std::pair<String, String>> query_params;

    String columns_to_get = "[";

    for (const auto& col : description.sample_block) {
        columns_to_get += quotify(col.name) + ",";
    }

    columns_to_get.back() = ']';

    query_params.push_back({quotify("attributesToRetrieve"), columns_to_get});
    query_params.push_back({quotify("limit"), std::to_string(max_block_size)});
    query_params.push_back({quotify("offset"), std::to_string(offset)});

    auto response = connection.searchQuery(query_params);

    JSON jres = JSON(response).begin();

    if (jres.getName() == "message") {
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());
    }

    size_t cnt_match = 0;
    String def;

    for (const auto kv_pair : jres.getValue()) {
        ++cnt_match;
        for (const auto idx : collections::range(0, size)) {
            const auto & name = description.sample_block.getByPosition(idx).name;
            std::cout << name << " " << description.sample_block.getByPosition(idx).type->getName() << "\n";
            auto value = kv_pair.getWithDefault(name, def);
            columns[idx]->insert(value);
        }
    }

    offset += cnt_match;

    if (cnt_match == 0) {
        all_read = true;
        return {};
    }

    return Chunk(std::move(columns), cnt_match);
}


}
