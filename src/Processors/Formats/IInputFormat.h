#pragma once

#include <Formats/ColumnMapping.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/InputFormatErrorsLogger.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Core/BlockMissingValues.h>


namespace DB
{

struct SelectQueryInfo;

using ColumnMappingPtr = std::shared_ptr<ColumnMapping>;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public SourceWithKeyCondition
{
protected:

    ReadBuffer * in [[maybe_unused]] = nullptr;

public:
    /// ReadBuffer can be nullptr for random-access formats.
    IInputFormat(Block header, ReadBuffer * in_);

    Chunk generate() override;

    /// All data reading from the read buffer must be performed by this method.
    virtual Chunk read() = 0;

    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a method
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream without recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual void setReadBuffer(ReadBuffer & in_);
    virtual void resetReadBuffer() { in = nullptr; }

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
    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;
};

using InputFormatPtr = std::shared_ptr<IInputFormat>;

}
