#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{

extern const SettingsBool allow_experimental_table_namespaces;

}

bool AddDefaultDatabaseVisitor::getAllowTableNamespaces(const ContextPtr & context)
{
    return context->getSettingsRef()[Setting::allow_experimental_table_namespaces];
}

}
