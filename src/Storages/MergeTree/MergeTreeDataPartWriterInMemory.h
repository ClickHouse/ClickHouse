#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>

namespace DB
{

/// Writes data part in memory.
class MergeTreeDataPartWriterInMemory : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterInMemory(
        const DataPartInMemoryPtr & part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeWriterSettings & settings_);

    /// You can write only one block. In-memory part can be written only at INSERT.
    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) override;

private:
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block);

    DataPartInMemoryPtr part_in_memory;
};

}
