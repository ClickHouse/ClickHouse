#pragma once

/// StorageIceberg depending on Avro to parse metadata with Avro format.
#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
struct IcebergMetadataParser
{
    static Strings getFiles(const Configuration & configuration, ContextPtr context);
};

}

#endif
