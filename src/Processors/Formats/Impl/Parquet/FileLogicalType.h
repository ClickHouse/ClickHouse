#pragma once

#include <optional>
#include <string_view>

namespace DB::Parquet
{

/// The Parquet `FILE` logical type annotates a group that references a range of bytes, stored either
/// inline or in an external file. The group may contain any subset of the following fields,
/// identified by name (case sensitively); every field present must be `OPTIONAL`.
/// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file
///
///   uri           BYTE_ARRAY (STRING)
///   offset        INT64
///   size          INT64
///   content_type  BYTE_ARRAY (STRING)
///   checksum      BYTE_ARRAY (STRING)
///   inline        BYTE_ARRAY
///
/// In ClickHouse such a group is a named `Tuple` of `Nullable(String)` and `Nullable(Int64)` elements.
enum class FileLogicalTypeFieldKind
{
    String, /// BYTE_ARRAY annotated with STRING
    Int64,  /// INT64
    Binary, /// BYTE_ARRAY without annotation
};

inline constexpr std::string_view file_logical_type_field_names = "uri, offset, size, content_type, checksum, inline";

inline std::optional<FileLogicalTypeFieldKind> getFileLogicalTypeFieldKind(std::string_view name)
{
    if (name == "uri" || name == "content_type" || name == "checksum")
        return FileLogicalTypeFieldKind::String;
    if (name == "offset" || name == "size")
        return FileLogicalTypeFieldKind::Int64;
    if (name == "inline")
        return FileLogicalTypeFieldKind::Binary;
    return std::nullopt;
}

}
