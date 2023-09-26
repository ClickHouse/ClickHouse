#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    using Protocol = ProxyConfiguration::Protocol;

    enum class ConnectProtocolPolicy
    {
        DEFAULT,
        FORCE_OFF,
        FORCE_ON
    };

    explicit ProxyConfigurationResolver(Protocol request_protocol_, ConnectProtocolPolicy connect_protocol_policy_ = ConnectProtocolPolicy::DEFAULT)
        : request_protocol(request_protocol_), connect_protocol_policy(connect_protocol_policy_)
    {
    }

    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve() = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;

protected:
    Protocol request_protocol;

    // TODO think about any case
    bool useConnectProtocol(Protocol proxy_protocol) const
    {
        switch (connect_protocol_policy)
        {
            case ConnectProtocolPolicy::DEFAULT:
                return ProxyConfiguration::Protocol::HTTPS == request_protocol && ProxyConfiguration::Protocol::HTTP == proxy_protocol;
            case ConnectProtocolPolicy::FORCE_OFF:
                return false;
            case ConnectProtocolPolicy::FORCE_ON:
                return true;
        }
    }

private:
    ConnectProtocolPolicy connect_protocol_policy;
};

}
