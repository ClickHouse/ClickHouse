#include <Storages/System/SystemTableSourceRegistry.h>

#include <string>
#include <typeindex>
#include <unordered_map>

namespace DB
{

namespace
{
    /// Function-local static to avoid static initialization order issues: the registrations run during dynamic
    /// initialization (from the `REGISTER_SYSTEM_TABLE_SOURCE` static objects in each system table's `.cpp`).
    std::unordered_map<std::type_index, const char *> & sourceRegistry()
    {
        static std::unordered_map<std::type_index, const char *> registry;
        return registry;
    }

    std::unordered_map<std::string, const char *> & documentationSourceRegistry()
    {
        static std::unordered_map<std::string, const char *> registry;
        return registry;
    }
}

void registerSystemTableSource(const std::type_info & type, const char * source_file)
{
    sourceRegistry()[std::type_index(type)] = source_file;
}

const char * getSystemTableSource(const std::type_info & type)
{
    const auto & registry = sourceRegistry();
    if (auto it = registry.find(std::type_index(type)); it != registry.end())
        return it->second;
    return nullptr;
}

void registerSystemTableDocumentationSource(std::string_view table_name, const char * source_file)
{
    documentationSourceRegistry()[std::string(table_name)] = source_file;
}

const char * getSystemTableDocumentationSource(std::string_view table_name)
{
    const auto & registry = documentationSourceRegistry();
    if (auto it = registry.find(std::string(table_name)); it != registry.end())
        return it->second;
    return nullptr;
}

}
