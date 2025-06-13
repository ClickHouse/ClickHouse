#pragma once

#include <base/types.h>
#include <Storages/IStorage.h>

namespace DB
{

StorageID tryParseTableIDFromDDL(const String & query, const String & default_database_name);

}
