#pragma once

#include <Client/BuzzHouse/Generator/SQLTypes.h>

namespace BuzzHouse
{

extern std::unique_ptr<SQLType> size_tp, null_tp;

extern std::unordered_map<String, std::unordered_map<String, SQLType *>> systemTables;

void loadSystemTables(bool has_cloud_features);

void clearSystemTables();

}
