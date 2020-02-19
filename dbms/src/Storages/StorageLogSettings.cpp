#include "StorageLogSettings.h"
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
String getDiskName(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        SettingsChanges changes = storage_def.settings->changes;
        for (auto it = changes.begin(); it != changes.end(); ++it)
        {
            if (it->name == "disk")
                return it->value.safeGet<String>();
        }
    }
    return "default";
}

}
