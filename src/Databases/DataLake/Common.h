#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

namespace DataLake
{

String trim(const String & str);

std::vector<String> splitTypeArguments(const String & type_str);

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "");

/// Parse a string, containing at least one dot, into a two substrings:
/// A.B.C.D.E -> A.B.C.D and E, where
/// `A.B.C.D` is a table "namespace".
/// `E` is a table name.
std::pair<std::string, std::string> parseTableName(const std::string & name);

}
