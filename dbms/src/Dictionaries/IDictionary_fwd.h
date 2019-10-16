#pragma once

#include <Core/Types.h>

#include <set>
#include <memory>

namespace DB
{

struct IDictionaryBase;
using DictionaryPtr = std::shared_ptr<IDictionaryBase>;

}
