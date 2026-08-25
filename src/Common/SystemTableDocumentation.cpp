#include <Common/SystemTableDocumentation.h>

#include <utility>


namespace DB
{

namespace
{

SystemTableDocumentationRegistry & documentationRegistry()
{
    static SystemTableDocumentationRegistry registry;
    return registry;
}

}

void registerSystemTableDocumentation(std::string_view table_name, SystemTableDocumentation documentation)
{
    documentationRegistry().insert_or_assign(String(table_name), std::move(documentation));
}

const SystemTableDocumentationRegistry & getSystemTableDocumentationRegistry()
{
    return documentationRegistry();
}

const SystemTableDocumentation * getSystemTableDocumentation(std::string_view table_name)
{
    const auto & registry = documentationRegistry();
    if (const auto it = registry.find(String(table_name)); it != registry.end())
        return &it->second;
    return nullptr;
}

}
