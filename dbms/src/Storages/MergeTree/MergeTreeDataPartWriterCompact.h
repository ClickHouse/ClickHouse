#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class MergeTreeDataPartWriterCompact : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterCompact(
        const String & part_path,
        const MergeTreeData & storage,
        const NamesAndTypesList & columns_list,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const WriterSettings & settings);

    MarkWithOffset write(const Block & block, const IColumn::Permutation * permutation,
        size_t from_mark, size_t index_offset, const MergeTreeIndexGranularity & index_granularity,
        const Block & primary_key_block, const Block & skip_indexes_block,
        bool skip_offsets = false, const WrittenOffsetColumns & already_written_offset_columns = {}) override;

    void finalize(IMergeTreeDataPart::Checksums & checksums, bool write_final_mark, bool sync = false) override;

private:
    /// Write single granule of one column (rows between 2 marks)
    size_t writeColumnSingleGranule(
        const ColumnWithTypeAndName & column,
        size_t from_row,
        size_t number_of_rows);

    ColumnStreamPtr stream;
};

}
