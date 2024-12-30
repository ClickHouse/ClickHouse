#pragma once

#include <SQLTypes.h>

namespace BuzzHouse
{

extern std::map<std::string, std::map<std::string, SQLType *>> systemTables;

void loadSystemTables(bool has_cloud_features);

void clearSystemTables();

}
