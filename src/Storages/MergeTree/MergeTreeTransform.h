#pragma once
#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeTransform : public ISimpleTransform
{
public:
    MergeTreeTransform(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ContextPtr & context);

    static Block transformHeader(Block header);

    String getName() const override { return "MergeTreeTransform"; }

    void transform(Chunk & chunk) override;

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    bool use_uncompressed_cache;
    DataPartsInfoPtr data_parts_info;
    NamesAndTypesList names_and_types_list;
};

}
