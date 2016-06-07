#pragma once

#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>


namespace DB
{

typedef std::vector<std::string> Names;
typedef std::unordered_set<std::string> NameSet;
typedef std::unordered_map<std::string, std::string> NameToNameMap;

}
