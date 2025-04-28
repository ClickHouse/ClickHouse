#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

namespace DataLake
{

String trim(const String & str);

std::vector<String> splitTypeArguments(const String & type_str);

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "");

}
