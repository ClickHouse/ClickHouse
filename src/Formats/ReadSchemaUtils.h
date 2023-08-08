#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/Cache/SchemaCache.h>
#include <Formats/FormatFactory.h>

namespace DB
{

using ReadBufferIterator = std::function<std::unique_ptr<ReadBuffer>(ColumnsDescription &)>;

/// Try to determine the schema of the data in the specified format.
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
    ReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context);

/// If ReadBuffer is created, it will be written to buf_out.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferIterator & read_buffer_iterator,
    bool retry,
    ContextPtr & context,
    std::unique_ptr<ReadBuffer> & buf_out);

SchemaCache::Key  getKeyForSchemaCache(const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);
SchemaCache::Keys  getKeysForSchemaCache(const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);

}
