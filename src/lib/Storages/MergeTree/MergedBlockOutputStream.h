#pragma once

#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Columns/ColumnArray.h>


namespace DB
{

/** To write one part.
  * The data refers to one partition, and is written in one part.
  */
class MergedBlockOutputStream final : public IMergedBlockOutputStream
{
public:
    MergedBlockOutputStream(
        MergeTreeData & storage_,
        const String & part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionCodecPtr default_codec_,
        bool blocks_are_granules_size_ = false);

    MergedBlockOutputStream(
        MergeTreeData & storage_,
        const String & part_path_,
        const NamesAndTypesList & columns_list_,
        CompressionCodecPtr default_codec_,
        const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
        size_t aio_threshold_,
        bool blocks_are_granules_size_ = false);

    std::string getPartPath() const;

    Block getHeader() const override { return storage.getSampleBlock(); }

    /// If the data is pre-sorted.
    void write(const Block & block) override;

    /** If the data is not sorted, but we have previously calculated the permutation, that will sort it.
      * This method is used to save RAM, since you do not need to keep two blocks at once - the original one and the sorted one.
      */
    void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

    void writeSuffix() override;

    /// Finilize writing part and fill inner structures
    void writeSuffixAndFinalizePart(
            MergeTreeData::MutableDataPartPtr & new_part,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

    const MergeTreeIndexGranularity & getIndexGranularity() const
    {
        return index_granularity;
    }

private:
    void init();

    /** If `permutation` is given, it rearranges the values in the columns when writing.
      * This is necessary to not keep the whole block in the RAM to sort it.
      */
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

private:
    NamesAndTypesList columns_list;

    size_t rows_count = 0;

    std::unique_ptr<WriteBufferFromFile> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
    MutableColumns index_columns;
    /// Index columns values from the last row from the last block
    /// It's written to index file in the `writeSuffixAndFinalizePart` method
    ColumnsWithTypeAndName last_index_row;
};

}
