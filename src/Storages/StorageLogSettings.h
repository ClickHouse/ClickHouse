#pragma once

#include <Storages/SettingDescription.h>

#include <memory>
#include <base/types.h>

namespace DB
{
    class ASTStorage;
    class Context;
    using ContextPtr = std::shared_ptr<const Context>;

    String getDiskName(ASTStorage & storage_def, ContextPtr context);

    struct StorageLogSettings
    {
        static bool hasBuiltin(std::string_view name);

        /// For `system.engine_settings`. The family keeps no settings struct, so the two are described here.
        static SettingDescriptions enumerateEngineSettings(ContextPtr context);
    };
}
