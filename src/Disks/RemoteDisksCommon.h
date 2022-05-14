#pragma once

#include <random>
#include <utility>
#include <Core/Types.h>
#include <Common/thread_local_rng.h>
#include <Disks/IDisk.h>
#include <Disks/DiskCacheWrapper.h>
#include <Common/getRandomASCIIString.h>


namespace DB
{

std::shared_ptr<DiskCacheWrapper> wrapWithCache(
    std::shared_ptr<IDisk> disk, String cache_name, String cache_path, String metadata_path);

std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context);

FileCachePtr getCachePtrForDisk(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context);

}
