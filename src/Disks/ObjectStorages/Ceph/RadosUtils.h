#pragma once
#include "config.h"

#if USE_CEPH
#include <map>
#include <Core/Types.h>
#include <base/types.h>
#include <librados.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct RadosEndpoint
{
    String mon_hosts;
    String pool;
    String nspace;
    String path;

    String getRelativePath() const { return path; }

    String getDescription() const { return mon_hosts + "/" + pool + "/" + getRelativePath(); }
};

struct RadosOptions : public std::map<String, String>
{
    /// Other information, including authentication, is stored in the options map
    String user;
    RadosOptions() { resetToDefaultOptions(); }
    void resetToDefaultOptions();
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    void validate();
};


}

#endif
