#pragma once
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
void loadDiskLocalConfig(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    String & path,
    UInt64 & keep_free_space_bytes);
}
