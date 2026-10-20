#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Poco/Dynamic/Var.h>

namespace DataLake
{

String trim(const String & str);

std::vector<String> splitTypeArguments(const String & type_str);

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "");

/// Converts an Iceberg field type, as it appears in `metadata.json` (either a primitive type
/// name such as `"long"` or `"decimal(9, 2)"`, or a nested object for `struct`/`list`/`map`),
/// into the Hive-style type string that AWS Glue stores in `StorageDescriptor.Columns[].Type`
/// (e.g. `bigint`, `decimal(9,2)`, or the nested `struct`/`array`/`map` spellings used by Hive).
///
/// The mapping follows the one used by Apache Iceberg's own Glue catalog implementation, so
/// tables written by ClickHouse are readable by Athena and other Glue consumers. It is the
/// inverse of `getType` above, which parses the same grammar when ClickHouse reads from Glue.
String icebergTypeToGlueType(const Poco::Dynamic::Var & type);

/// Parse a string, containing at least one dot, into a two substrings:
/// A.B.C.D.E -> A.B.C.D and E, where
/// `A.B.C.D` is a table "namespace".
/// `E` is a table name.
std::pair<std::string, std::string> parseTableName(const std::string & name);

}
