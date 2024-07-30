#pragma once
#include "config.h"

#if USE_CEPH
#include <map>
#include <librados.hpp>
#include <base/types.h>
#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct RadosEndpoint
{
    String mon_hosts;
    String pool;
    String nspace;
    String snapshot;
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
