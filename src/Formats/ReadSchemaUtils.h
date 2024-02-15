#pragma once

#include <Formats/FormatFactory.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct IReadBufferIterator
{
    virtual ~IReadBufferIterator() = default;

    virtual void setPreviousReadBuffer(std::unique_ptr<ReadBuffer> /* buffer */) {}

    /// Return read buffer of the next file or cached schema.
    /// In DEFAULT schema inference mode cached schema can be from any file.
    /// In UNION mode cached schema can be only from current file.
    /// When there is no files to process, return pair (nullptr, nullopt)
    virtual std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() = 0;

    virtual void setNumRowsToLastFile(size_t /*num_rows*/) {}

    /// Set schema inferred from last file. Used for UNION mode to cache schema
    /// per file.
    virtual void setSchemaToLastFile(const ColumnsDescription & /*columns*/) {}
    /// Set resulting inferred schema. Used for DEFAULT mode to cache schema
    /// for all files.
    virtual void setResultingSchema(const ColumnsDescription & /*columns*/) {}

    /// Get last processed file name for better exception messages.
    virtual String getLastFileName() const { return ""; }
};

struct SingleReadBufferIterator : public IReadBufferIterator
{
public:
    explicit SingleReadBufferIterator(std::unique_ptr<ReadBuffer> buf_) : buf(std::move(buf_))
    {
    }

    std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
    {
        if (done)
            return {nullptr, {}};
        done = true;
        return {std::move(buf), {}};
    }

private:
    std::unique_ptr<ReadBuffer> buf;
    bool done = false;
};

/// Try to determine the schema of the data and number of rows in data in the specified format.
/// For formats that have an external schema reader, it will
/// use it and won't create a read buffer.
/// For formats that have a schema reader from the data,
/// read buffer will be created by the provided iterator and
/// the schema will be extracted from the data. If the format doesn't
/// have any schema reader an exception will be thrown.
/// Reading schema can be performed in 2 modes depending on setting schema_inference_mode:
/// 1) Default mode. In this mode ClickHouse assumes that all files have the same schema
/// and tries to infer the schema by reading files one by one until it succeeds.
/// If schema reader couldn't determine the schema for some file, ClickHouse will try the next
/// file (next read buffer from the provided iterator) if it makes sense. If ClickHouse couldn't determine
/// the resulting schema, an exception will be thrown.
/// 2) Union mode. In this mode ClickHouse assumes that files can have different schemas,
/// so it infer schemas of all files and then union them to the common schema. In this mode
/// all read buffers from provided iterator will be used. If ClickHouse couldn't determine
/// the schema for some file, an exception will be thrown.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context);

/// If ReadBuffer is created, it will be written to buf_out.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context,
    std::unique_ptr<ReadBuffer> & buf_out);

SchemaCache::Key getKeyForSchemaCache(const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);
SchemaCache::Keys getKeysForSchemaCache(const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);

}
