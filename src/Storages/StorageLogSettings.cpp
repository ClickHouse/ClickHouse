#include "StorageLogSettings.h"
#include <Disks/StoragePolicy.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
String getDiskName(ASTStorage & storage_def, ContextPtr context)
{
    if (storage_def.settings)
    {
        SettingsChanges changes = storage_def.settings->changes;
        for (const auto & change : changes)
        {
            /// How about both disk and storage_policy are specified?
            if (change.name == "disk")
                return change.value.safeGet<String>();
            if (change.name == "storage_policy")
            {
                auto policy = context->getStoragePolicy(change.value.safeGet<String>());
                return policy->getAnyDisk()->getName();
            }
        }
    }
    return "default";
}

}
