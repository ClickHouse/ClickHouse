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
        const MergeTreeWriterSettings & settings_);

    void write(const Block & block, const IColumn::Permutation * permutation,
        const Block & primary_key_block, const Block & skip_indexes_block) override;

    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums) override;

    void calculateAndSerializePrimaryIndex(const Block & primary_index_block) override;

private:
    DataPartInMemoryPtr part;
    bool block_written = false;
};

}
