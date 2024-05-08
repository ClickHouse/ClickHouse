#pragma once

#include <cstddef>
#include <unordered_map>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/ExternalResultDescription.h>
#include <Processors/Chunk.h>
#include <Processors/ISource.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <base/JSON.h>

namespace DB
{
class MeiliSearchSource final : public ISource
{
public:
    enum QueryRoute
    {
        search,
        documents
    };

    MeiliSearchSource(
        const MeiliSearchConfiguration & config,
        const Block & sample_block,
        UInt64 max_block_size_,
        QueryRoute route,
        std::unordered_map<String, String> query_params_);

    ~MeiliSearchSource() override;

    String getName() const override { return "MeiliSearchSource"; }

private:
    String doubleQuoteIfNeed(const String & param) const;

    String constructAttributesToRetrieve() const;

    size_t parseJSON(MutableColumns & columns, const JSON & jres) const;

    Chunk generate() override;

    MeiliSearchConnection connection;
    const UInt64 max_block_size;
    const QueryRoute route;
    ExternalResultDescription description;
    std::unordered_map<String, String> query_params;

    UInt64 offset;
    bool all_read = false;
};

}
