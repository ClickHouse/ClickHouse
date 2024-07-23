#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
struct HudiMetadataParser
{
public:
    HudiMetadataParser<Configuration, MetadataReadHelper>();

    Strings getFiles(const Configuration & configuration, ContextPtr context);

private:
    struct Impl;
    std::shared_ptr<Impl> impl;
};

}
