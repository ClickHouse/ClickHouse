#pragma once

#include "config.h"

#include <string>
#include <vector>


#include <Common/LatencyBuckets.h>
#include <Common/RemoteHostFilter.h>
#include <Common/IThrottler.h>
#include <Common/ProxyConfiguration.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/SessionAwareIOStream.h>

namespace DB
{

class RemoteLLMSDK
{
public:
    RemoteLLMSDK() = default;
    ~RemoteLLMSDK() = default;
    std::string complete(Poco::URL & url, )
};
}

