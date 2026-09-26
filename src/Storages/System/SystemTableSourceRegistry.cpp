#include <Storages/System/SystemTableSourceRegistry.h>

#include <mutex>
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

    struct DocumentationSourceRegistry
    {
        std::mutex mutex;
        std::unordered_map<std::string, const char *> by_table_name;
        std::unordered_map<std::string, const char *> by_comment;
    };

    DocumentationSourceRegistry & documentationSourceRegistry()
    {
        static DocumentationSourceRegistry registry;
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

void registerSystemTableDocumentationSource(
    std::string_view table_name,
    const char * source_file,
    std::string_view documentation)
{
    auto & registry = documentationSourceRegistry();
    std::lock_guard lock(registry.mutex);
    registry.by_table_name[std::string(table_name)] = source_file;
    if (!documentation.empty())
        registry.by_comment[std::string(documentation)] = source_file;
}

const char * getSystemTableDocumentationSource(std::string_view table_name)
{
    auto & registry = documentationSourceRegistry();
    std::lock_guard lock(registry.mutex);
    if (auto it = registry.by_table_name.find(std::string(table_name)); it != registry.by_table_name.end())
        return it->second;
    return nullptr;
}

const char * getSystemTableDocumentationSourceFromComment(std::string_view comment)
{
    auto & registry = documentationSourceRegistry();
    std::lock_guard lock(registry.mutex);

    const char * source = nullptr;
    size_t longest_match = 0;
    for (const auto & [documentation, documentation_source] : registry.by_comment)
    {
        if (documentation.size() > longest_match && comment.starts_with(documentation))
        {
            source = documentation_source;
            longest_match = documentation.size();
        }
    }
    return source;
}

}
