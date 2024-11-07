#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct IReadBufferIterator
{
    virtual ~IReadBufferIterator() = default;

    /// Return read buffer of the next file or cached schema.
    /// In DEFAULT schema inference mode cached schema can be from any file.
    /// In UNION mode cached schema can be only from current file.
    /// When there is no files to process, return pair (nullptr, nullopt)

    struct Data
    {
        /// Read buffer of the next file. Can be nullptr if there are no more files
        /// or when schema was found in cache.
        std::unique_ptr<ReadBuffer> buf;

        /// Schema from cache.
        /// In DEFAULT schema inference mode cached schema can be from any file.
        /// In UNION mode cached schema can be only from current file.
        std::optional<ColumnsDescription> cached_columns;

        /// Format of the file if known.
        std::optional<String> format_name;
    };

    virtual Data next() = 0;

    /// Set read buffer returned in previous iteration.
    virtual void setPreviousReadBuffer(std::unique_ptr<ReadBuffer> /* buffer */) {}

    /// Set number of rows to last file extracted during schema inference.
    /// Used for caching number of rows from files metadata during schema inference.
    virtual void setNumRowsToLastFile(size_t /*num_rows*/) {}

    /// Set schema inferred from last file. Used for UNION mode to cache schema
    /// per file.
    virtual void setSchemaToLastFile(const ColumnsDescription & /*columns*/) {}

    /// Set resulting inferred schema. Used for DEFAULT mode to cache schema
    /// for all files.
    virtual void setResultingSchema(const ColumnsDescription & /*columns*/) {}

    /// Set auto detected format name.
    virtual void setFormatName(const String & /*format_name*/) {}

    /// Get last processed file path for better exception messages.
    virtual String getLastFilePath() const { return ""; }

    /// Return true if method recreateLastReadBuffer is implemented.
    virtual bool supportsLastReadBufferRecreation() const { return false; }

    /// Recreate last read buffer to read data from the same file again.
    /// Used to detect format from the file content to avoid
    /// copying data.
    virtual std::unique_ptr<ReadBuffer> recreateLastReadBuffer()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method recreateLastReadBuffer is not implemented");
    }
};

struct SingleReadBufferIterator : public IReadBufferIterator
{
public:
    explicit SingleReadBufferIterator(std::unique_ptr<ReadBuffer> buf_) : buf(std::move(buf_))
    {
    }

    Data next() override
    {
        if (done)
            return {nullptr, {}, std::nullopt};
        done = true;
        return Data{std::move(buf), {}, std::nullopt};
    }

    void setPreviousReadBuffer(std::unique_ptr<ReadBuffer> buf_) override
    {
        buf = std::move(buf_);
    }

    std::unique_ptr<ReadBuffer> releaseBuffer()
    {
        return std::move(buf);
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
    const ContextPtr & context);

/// Try to detect the format of the data and it's schema.
/// It runs schema inference for some set of formats on the same file.
/// If schema reader of some format successfully inferred the schema from
/// some file, we consider that the data is in this format.
std::pair<ColumnsDescription, String> detectFormatAndReadSchema(
    const std::optional<FormatSettings> & format_settings,
    IReadBufferIterator & read_buffer_iterator,
    const ContextPtr & context);

SchemaCache::Key getKeyForSchemaCache(const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);
SchemaCache::Keys getKeysForSchemaCache(const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);

}
