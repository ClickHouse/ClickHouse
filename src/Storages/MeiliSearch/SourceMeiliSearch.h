#pragma once

#include <Processors/Chunk.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Core/ExternalResultDescription.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{

class MeiliSearchSource final : public SourceWithProgress
{
public:
    MeiliSearchSource(
        const MeiliSearchConfiguration& config,
        const Block & sample_block,
        UInt64 max_block_size_,
        UInt64 offset_);

    ~MeiliSearchSource() override;

    String getName() const override { return "MeiliSearchSource"; }

private:
    Chunk generate() override;

    MeiliSearchConnection connection;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    UInt64 offset;
    bool all_read = false;
};

}
