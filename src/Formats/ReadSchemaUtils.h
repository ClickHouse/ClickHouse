#pragma once

#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFactory.h>

namespace DB
{

using ReadBufferIterator = std::function<std::unique_ptr<ReadBuffer>(ColumnsDescription &)>;

/// Try to determine the schema of the data in specifying format.
/// For formats that have an external schema reader, it will
/// use it and won't create a read buffer.
/// For formats that have a schema reader from the data,
/// read buffer will be created by the provided iterator and
/// the schema will be extracted from the data. If schema reader
/// couldn't determine the schema we will try the next read buffer
/// from provided iterator if it makes sense. If format doesn't
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

/// Make type Nullable recursively:
/// - Type -> Nullable(type)
/// - Array(Type) -> Array(Nullable(Type))
/// - Tuple(Type1, ..., TypeN) -> Tuple(Nullable(Type1), ..., Nullable(TypeN))
/// - Map(KeyType, ValueType) -> Map(KeyType, Nullable(ValueType))
/// - LowCardinality(Type) -> LowCardinality(Nullable(Type))
/// If type is Nothing or one of the nested types is Nothing, return nullptr.
DataTypePtr makeNullableRecursivelyAndCheckForNothing(DataTypePtr type);

/// Call makeNullableRecursivelyAndCheckForNothing for all types
/// in the block and return names and types.
NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header);

String getKeyForSchemaCache(const String & source, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);
Strings getKeysForSchemaCache(const Strings & sources, const String & format, const std::optional<FormatSettings> & format_settings, const ContextPtr & context);

void splitSchemaCacheKey(const String & key, String & source, String & format, String & additional_format_info);
}
