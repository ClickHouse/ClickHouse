#pragma once
#include "config.h"

#if USE_CEPH
#include <map>
#include <base/types.h>
#include <rados/librados.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct CephEndpoint
{
    String mon_hosts;
    String pool;
    String snapshot;
    // String key;
};

struct CephOptions : public std::map<String, String>
{
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
};


}

#endif
