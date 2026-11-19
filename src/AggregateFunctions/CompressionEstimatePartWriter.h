#pragma once

#include <Compression/CompressedSizeCalculator.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <memory>
#include <optional>


namespace DB
{

class IColumn;

/// Writes one column of a wide part of a new `MergeTree` table with the default settings, to `CompressedSizeCalculator`s
/// instead of files, for its compressed size.
class CompressionEstimatePartWriter
{
public:
    /// Shared by the parts of one function; resolves the stream codecs once.
    class Layout
    {
    public:
        /// `block_size_bytes` replaces both `min_compress_block_size` and `max_compress_block_size`.
        Layout(DataTypePtr target_type_, ASTPtr codec_description_, std::optional<UInt64> block_size_bytes);

        size_t granuleRows() const { return granule_rows; }

    private:
        friend class CompressionEstimatePartWriter;

        struct Stream
        {
            CompressionCodecPtr codec;
            size_t block_size;
        };

        Stream resolveStream(const ISerialization::SubstreamPath & path) const;

        DataTypePtr target_type;
        ASTPtr codec_description;
        size_t granule_rows;
        size_t granule_bytes;
        size_t min_compress_block_size;
        size_t max_compress_block_size;
        UnorderedMapWithMemoryTracking<String, Stream> streams;
    };

    /// `first_rows` decide whether the column is sparse, as the rows of an insert do for a new part. They are not written.
    CompressionEstimatePartWriter(const Layout & layout, const IColumn & first_rows);
    ~CompressionEstimatePartWriter();

    CompressionEstimatePartWriter(const CompressionEstimatePartWriter &) = delete;
    CompressionEstimatePartWriter & operator=(const CompressionEstimatePartWriter &) = delete;

    size_t rowsLeftInGranule() const { return granule_rows_limit - granule_rows; }

    /// Takes at most `rowsLeftInGranule` rows. After an exception the writer can only be destroyed.
    void write(const IColumn & rows);

    /// Returns the compressed bytes of the part. Nothing can be written after it.
    UInt64 finish();

private:
    std::pair<size_t, size_t> rowsToGranuleEnd(const IColumn & rows, size_t offset) const;

    SerializationPtr serialization;
    ISerialization::SerializeBinaryBulkSettings settings;
    ISerialization::SerializeBinaryBulkStatePtr state;
    UnorderedMapWithMemoryTracking<String, std::unique_ptr<CompressedSizeCalculator>> streams;
    const size_t granule_rows_limit;
    const size_t granule_bytes_limit;
    const size_t min_compress_block_size;
    /// Of the open granule.
    size_t granule_rows = 0;
    size_t granule_bytes = 0;
};

}
