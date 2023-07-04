#pragma once

#include <random>
#include <utility>

#include <Core/Types.h>
#include <Common/thread_local_rng.h>

#include <Disks/IDisk.h>


namespace DB
{

std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context);

bool isFileWithPersistentCache(const String & path);

}
