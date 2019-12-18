#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <DataStreams/SquashingTransform.h>

namespace DB
{

class MergeTreeDataPartWriterCompact : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterCompact(
        const String & part_path,
        const MergeTreeData & storage,
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc, 
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    void write(const Block & block, const IColumn::Permutation * permutation = nullptr,
        const Block & primary_key_block = {}, const Block & skip_indexes_block = {}) override;

    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync = false) override;

private:
    /// Write single granule of one column (rows between 2 marks)
    size_t writeColumnSingleGranule(
        const ColumnWithTypeAndName & column,
        size_t from_row,
        size_t number_of_rows);

    void writeBlock(const Block & block);

    ColumnStreamPtr stream;

    SquashingTransform squashing;
    Block header;
};

}
