#pragma once

#include <Client/BuzzHouse/Generator/SQLTypes.h>

namespace BuzzHouse
{

extern SQLType *size_tp, *null_tp;

extern std::unordered_map<std::string, std::unordered_map<std::string, SQLType *>> systemTables;

void loadSystemTables(bool has_cloud_features);

void clearSystemTables();

}
