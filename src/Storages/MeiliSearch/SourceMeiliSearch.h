#pragma once

#include <unordered_map>
#include <Core/ExternalResultDescription.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{
class MeiliSearchSource final : public SourceWithProgress
{
public:
    MeiliSearchSource(
        const MeiliSearchConfiguration & config,
        const Block & sample_block,
        UInt64 max_block_size_,
        std::unordered_map<String, String> query_params_);

    ~MeiliSearchSource() override;

    String getName() const override { return "MeiliSearchSource"; }

private:
    Chunk generate() override;

    MeiliSearchConnection connection;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    std::unordered_map<String, String> query_params;

    UInt64 offset;
    bool all_read = false;
};

}
