#pragma once

#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFactory.h>

namespace DB
{

/// Try to determine the schema of the data in specifying format.
/// For formats that have an external schema reader, it will
/// use it and won't create a read buffer.
/// For formats that have a schema reader from the data,
/// read buffer will be created by the provided creator and
/// the schema will be extracted from the data.
/// If format doesn't have any schema reader or a schema reader
/// couldn't determine the schema, an exception will be thrown.
using ReadBufferCreator = std::function<std::unique_ptr<ReadBuffer>()>;
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferCreator read_buffer_creator,
    ContextPtr context);

/// If ReadBuffer is created, it will be written to buf_out.
ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferCreator read_buffer_creator,
    ContextPtr context,
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
}
