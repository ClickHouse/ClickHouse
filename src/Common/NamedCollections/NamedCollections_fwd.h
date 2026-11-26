#pragma once
#include <functional>
#include <memory>
#include <map>
#include <string>

namespace DB
{

class NamedCollection;
using NamedCollectionPtr = std::shared_ptr<const NamedCollection>;
using MutableNamedCollectionPtr = std::shared_ptr<NamedCollection>;
using NamedCollectionsMap = std::map<std::string, MutableNamedCollectionPtr>;
/// Returns the named collection that a name resolves to instead of the one that this server has, or nullptr for that one, such as
/// a collection that `RESTORE` installs before it creates a definition that uses it.
using GetNamedCollectionFn = std::function<NamedCollectionPtr(const std::string & collection_name)>;

}
