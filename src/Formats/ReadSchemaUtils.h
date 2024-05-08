#pragma once

#include <Formats/FormatFactory.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct IReadBufferIterator
{
    virtual ~IReadBufferIterator() = default;

    virtual std::unique_ptr<ReadBuffer> next() = 0;

    virtual std::optional<ColumnsDescription> getCachedColumns() { return std::nullopt; }

    virtual void setNumRowsToLastFile(size_t /*num_rows*/) {}
};

struct SingleReadBufferIterator : public IReadBufferIterator
{
public:
    SingleReadBufferIterator(std::unique_ptr<ReadBuffer> buf_) : buf(std::move(buf_))
    {
    }

    std::unique_ptr<ReadBuffer> next() override
    {
        if (done)
            return nullptr;
        done = true;
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
/// the schema will be extracted from the data. If schema reader
/// couldn't determine the schema we will try the next read buffer
/// from the provided iterator if it makes sense. If the format doesn't
/// have any schema reader or we couldn't determine the schema,
/// an exception will be thrown.
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
