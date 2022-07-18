#pragma once

#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Columns/ColumnArray.h>
#include <IO/WriteSettings.h>


namespace DB
{

/** To write one part.
  * The data refers to one partition, and is written in one part.
  */
class MergedBlockOutputStream final : public IMergedBlockOutputStream
{
public:
    MergedBlockOutputStream(
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_,
        const NamesAndTypesList & columns_list_,
        const MergeTreeIndices & skip_indices,
        CompressionCodecPtr default_codec_,
        const MergeTreeTransactionPtr & txn,
        bool reset_columns_ = false,
        bool blocks_are_granules_size = false,
        const WriteSettings & write_settings = {});

    Block getHeader() const { return metadata_snapshot->getSampleBlock(); }

    /// If the data is pre-sorted.
    void write(const Block & block) override;

    /** If the data is not sorted, but we have previously calculated the permutation, that will sort it.
      * This method is used to save RAM, since you do not need to keep two blocks at once - the original one and the sorted one.
      */
    void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation);

    /// Finalizer is a structure which is returned from by finalizePart().
    /// Files from part may be written asynchronously, e.g. for blob storages.
    /// You should call finish() to wait until all data is written.
    struct Finalizer
    {
        struct Impl;
        std::unique_ptr<Impl> impl;

        explicit Finalizer(std::unique_ptr<Impl> impl_);
        ~Finalizer();
        Finalizer(Finalizer &&) noexcept;
        Finalizer & operator=(Finalizer &&) noexcept;

        void finish();
    };

    /// Finalize writing part and fill inner structures
    /// If part is new and contains projections, they should be added before invoking this method.
    Finalizer finalizePartAsync(
            MergeTreeData::MutableDataPartPtr & new_part,
            bool sync,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr,
            const WriteSettings & settings = {});

    void finalizePart(
            MergeTreeData::MutableDataPartPtr & new_part,
            bool sync,
            const NamesAndTypesList * total_columns_list = nullptr,
            MergeTreeData::DataPart::Checksums * additional_column_checksums = nullptr);

private:
    /** If `permutation` is given, it rearranges the values in the columns when writing.
      * This is necessary to not keep the whole block in the RAM to sort it.
      */
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

    using WrittenFiles = std::vector<std::unique_ptr<WriteBufferFromFileBase>>;
    WrittenFiles finalizePartOnDisk(
            const MergeTreeData::DataPartPtr & new_part,
            MergeTreeData::DataPart::Checksums & checksums,
            const WriteSettings & write_settings);

    NamesAndTypesList columns_list;
    IMergeTreeDataPart::MinMaxIndex minmax_idx;
    size_t rows_count = 0;
    CompressionCodecPtr default_codec;
};

using MergedBlockOutputStreamPtr = std::shared_ptr<MergedBlockOutputStream>;

}
