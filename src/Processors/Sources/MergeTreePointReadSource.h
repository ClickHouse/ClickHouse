#pragma once

#include <Processors/ISource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Core/NamesAndTypes.h>
#include <Common/PODArray.h>

namespace DB
{

class CompressedReadBufferFromFile;

/// Point-reads a fixed-size, uncompressed (`CODEC(NONE)`) `Array` column for a set of exact row offsets,
/// fetching each row's single compressed block instead of decompressing whole granules. Used by lazy
/// materialization for the two-phase quantized-codes vector search rescore.
///
/// Precondition (established by the CREATE-time block-size alignment and re-checked by `isEligible`): the
/// column's element stream is written as one vector per compressed block (`max_compress_block_size == row_size`).
/// Then the element `.bin` is a flat sequence of equal blocks, so block/row `r` is at
/// `r * (framing + row_size)` where `framing = checksum(16) + header(9) = 25`. Checksums are kept: each block
/// is read and verified in full (a block is exactly one row here).
class MergeTreePointReadSource final : public ISource
{
public:
    MergeTreePointReadSource(
        SharedHeader header_,
        RangesInDataPart part_,
        PaddedPODArray<UInt64> row_offsets_,
        NameAndTypePair column_,
        size_t dimensions_,
        MergeTreeReaderSettings reader_settings_,
        size_t max_block_size_);

    ~MergeTreePointReadSource() override;

    String getName() const override { return "MergeTreePointReadSource"; }

    /// Returns true if `part`'s `column` is stored with a one-row-per-block layout so point reads are exact.
    /// Definitive and cheap: the element `.bin` size equals `rows_count * (framing + row_size)` iff every block
    /// holds exactly one vector.
    static bool isEligible(const RangesInDataPart & part, const NameAndTypePair & column, size_t dimensions);

protected:
    Chunk generate() override;

private:
    void initialize();

    RangesInDataPart part;
    PaddedPODArray<UInt64> row_offsets;
    NameAndTypePair column;
    size_t dimensions;
    size_t element_size = 0; /// bytes per vector element
    size_t row_size = 0;     /// dimensions * element_size (bytes of one vector in the element stream)
    size_t block_stride = 0; /// on-disk bytes of one 1-row block: framing + row_size
    MergeTreeReaderSettings reader_settings;
    size_t max_block_size;

    bool initialized = false;
    size_t next_offset_index = 0;
    String element_file_name;
    std::unique_ptr<CompressedReadBufferFromFile> data_buffer;
};

}
