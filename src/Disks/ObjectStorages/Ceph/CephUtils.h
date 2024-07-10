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

struct CephEndpoint
{
    String mon_hosts;
    String pool;
    String nspace;
    String snapshot;
};

struct CephOptions : public std::map<String, String>
{
    /// Other information, including authentication, is stored in the options map
    String user;
    CephOptions() { resetToDefaultOptions(); }
    void resetToDefaultOptions();
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    void validate();
};


}

#endif
