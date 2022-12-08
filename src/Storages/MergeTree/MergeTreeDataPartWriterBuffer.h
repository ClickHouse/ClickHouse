#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartBuffer.h>

namespace DB
{

/// Writes Buffer data part.
/// FIXME: duplicate of MergeTreeReaderInMemory
class MergeTreeDataPartWriterBuffer : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterBuffer(
        const MutableDataPartBufferPtr & part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeWriterSettings & settings_);

    /// You can write only one block. Buffer part can be written only at INSERT.
    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void fillChecksums(IMergeTreeDataPart::Checksums & checksums) override;
    void finish(bool /*sync*/) override {}

private:
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block);

    MutableDataPartBufferPtr part_buffer;
};

}
