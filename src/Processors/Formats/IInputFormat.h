#pragma once

#include <Formats/ColumnMapping.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/InputFormatErrorsLogger.h>
#include <Common/PODArray.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <Core/BlockMissingValues.h>
#include <Processors/ISource.h>


namespace DB
{

struct SelectQueryInfo;

using ColumnMappingPtr = std::shared_ptr<ColumnMapping>;
using IColumnFilter = PaddedPODArray<UInt8>;

/// Most (all?) file formats have a natural order of rows within the file.
/// But our format readers and query pipeline may reorder or filter rows. This struct is used to
/// propagate the original row numbers, e.g. for _row_number virtual column or for iceberg
/// positional deletes.
///
/// Warning: we currently don't correctly update this info in most transforms. E.g. things like
/// FilterTransform and SortingTransform logically should remove this ChunkInfo, but don't; we don't
/// have a mechanism to systematically find all code sites that would need to do that or to detect
/// if one was missed.
/// So this is only used in a few specific situations, and the builder of query pipeline must be
/// careful to never put a step that uses this info after a step that breaks it.
///
/// If row numbers in a chunk are consecutive, this contains just the first row number.
/// If row numbers are not consecutive as a result of filtering, this additionally contains the mask
/// that was used for filtering, from which row numbers can be recovered.
struct ChunkInfoRowNumbers : public ChunkInfo
{
    explicit ChunkInfoRowNumbers(size_t row_num_offset_, std::optional<IColumnFilter> applied_filter_ = std::nullopt);

    Ptr clone() const override;

    const size_t row_num_offset;
    /// If nullopt, row numbers are consecutive.
    /// If not empty, the number of '1' elements is equal to the number of rows in the chunk;
    /// row i in the chunk has row number:
    /// row_num_offset + {index of the i-th '1' element in applied_filter}.
    std::optional<IColumnFilter> applied_filter;
};

/// Structure for storing information about buckets that IInputFormat needs to read.
struct FileBucketInfo
{
    virtual void serialize(WriteBuffer & buffer) = 0;
    virtual void deserialize(ReadBuffer & buffer) = 0;
    virtual String getIdentifier() const = 0;
    virtual String getFormatName() const = 0;

    virtual ~FileBucketInfo() = default;
};
using FileBucketInfoPtr = std::shared_ptr<FileBucketInfo>;

/// Interface for splitting a file into buckets.
struct IBucketSplitter
{
    /// Splits a file into buckets using the given read buffer and format settings.
    /// Returns information about the resulting buckets (see the structure above for details).
    virtual std::vector<FileBucketInfoPtr> splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_) = 0;

    virtual ~IBucketSplitter() = default;
};
using BucketSplitter = std::shared_ptr<IBucketSplitter>;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
protected:

    /// Note: implementations should prefer to drain this ReadBuffer to the end if it's not seekable
    /// (unless it would cause too much extra IO). That's because `in` may be reading HTTP POST data
    /// from the socket, and if not all data is read then the connection can't be reused for later
    /// HTTP requests (keepalive).
    ReadBuffer * in [[maybe_unused]] = nullptr;

public:
    /// ReadBuffer can be nullptr for random-access formats.
    IInputFormat(SharedHeader header, ReadBuffer * in_);

    Chunk generate() override;

    void onFinish() override;

    /// All data reading from the read buffer must be performed by this method.
    virtual Chunk read() = 0;

    virtual void setBucketsToRead(const FileBucketInfoPtr & buckets_to_read);
    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a method
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream without recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual void setReadBuffer(ReadBuffer & in_);
    virtual void resetReadBuffer() { in = nullptr; resetOwnedBuffers(); }

    virtual const BlockMissingValues * getMissingValues() const { return nullptr; }

    /// Must be called from ParallelParsingInputFormat after readSuffix
    ColumnMappingPtr getColumnMapping() const { return column_mapping; }
    /// Must be called from ParallelParsingInputFormat before readPrefix
    void setColumnMapping(ColumnMappingPtr column_mapping_) { column_mapping = column_mapping_; }

    /// Set the number of rows that was already read in
    /// parallel parsing before creating this parser.
    virtual void setRowsReadBefore(size_t /*rows*/) {}

    /// Sets the serialization hints for the columns. It allows to create columns
    /// in custom serializations (e.g. Sparse) for parsing and avoid extra conversion.
    virtual void setSerializationHints(const SerializationInfoByName & /*hints*/) {}

    void addBuffer(std::unique_ptr<ReadBuffer> buffer) { owned_buffers.emplace_back(std::move(buffer)); }

    void setErrorsLogger(const InputFormatErrorsLoggerPtr & errors_logger_) { errors_logger = errors_logger_; }

    virtual size_t getApproxBytesReadForChunk() const { return 0; }

    void needOnlyCount() { need_only_count = true; }

protected:
    ReadBuffer & getReadBuffer() const { chassert(in); return *in; }

    virtual Chunk getChunkForCount(size_t rows);

    ColumnMappingPtr column_mapping{};

    InputFormatErrorsLoggerPtr errors_logger;

    bool need_only_count = false;

private:
    void resetOwnedBuffers();

    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;
};

using InputFormatPtr = std::shared_ptr<IInputFormat>;

}
