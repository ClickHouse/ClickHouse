#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

using AliasToName = std::unordered_map<std::string, std::string>;
using AliasToNamePtr = std::shared_ptr<AliasToName>;

class MergeTreeTransform : public ISimpleTransform
{
public:
    MergeTreeTransform(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ContextPtr & context_,
        const AliasToNamePtr & alias_index_);

    static Block transformHeader(Block header);

    String getName() const override { return "MergeTreeTransform"; }

    void transform(Chunk & chunk) override;

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    bool use_uncompressed_cache;
    DataPartsInfoPtr data_parts_info;
    Names requested_column_names;
    Names alias_column_names;
};

}
