#pragma once

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
struct IcebergMetadataParser
{
public:
    IcebergMetadataParser<Configuration, MetadataReadHelper>();

    Strings getFiles(const Configuration & configuration, ContextPtr context);

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
};

}

#endif
