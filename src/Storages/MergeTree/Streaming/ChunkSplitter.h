#pragma once

#include <Processors/ISimpleTransform.h>

#include <Storages/MergeTree/Streaming/CursorUtils.h>

namespace DB
{

class ChunkSplitterTransform : public ISimpleTransform
{
public:
    ChunkSplitterTransform(Block header_, MergeTreeCursor cursor_, String storage_full_name_, std::optional<String> keeper_key_);

    String getName() const override { return "ChunkSplitterTransform"; }

protected:
    void transform(Chunk & chunk) override;

    Chunk splitByCursor(Chunk && chunk, const PartitionCursor & current);

    PartitionCursor getCursor(const Chunk & chunk, size_t index) const;

private:
    MergeTreeCursor cursor;
    String storage_full_name;
    std::optional<String> keeper_key;

    size_t block_number_column_index = 0;
    size_t block_offset_column_index = 0;
};

}
