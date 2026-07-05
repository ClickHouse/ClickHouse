#pragma once
#include <memory>
#include <map>

namespace DB
{

class NamedCollection;
using NamedCollectionPtr = std::shared_ptr<const NamedCollection>;
using MutableNamedCollectionPtr = std::shared_ptr<NamedCollection>;
using NamedCollectionsMap = std::map<std::string, MutableNamedCollectionPtr>;

}
