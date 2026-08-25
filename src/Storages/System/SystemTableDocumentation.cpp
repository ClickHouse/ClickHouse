#include <Storages/System/SystemTableDocumentation.h>

#include <unordered_map>


namespace DB
{

namespace
{

using DocumentationRegistry = std::unordered_map<std::string_view, SystemTableDocumentation>;

const DocumentationRegistry & documentationRegistry()
{
    static const DocumentationRegistry registry = {
#include <Storages/System/SystemTableDocumentation.inc>
    };
    return registry;
}

}

const SystemTableDocumentation * getSystemTableDocumentation(std::string_view table_name)
{
    const auto & registry = documentationRegistry();
    if (const auto it = registry.find(table_name); it != registry.end())
        return &it->second;
    return nullptr;
}

}
