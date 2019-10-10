#pragma once

#include <Core/Types.h>

#include <map>
#include <memory>

namespace DB
{

struct IDictionaryBase;
using DictionaryPtr = std::shared_ptr<IDictionaryBase>;
using Dictionaries = std::map<String, DictionaryPtr>;

}
