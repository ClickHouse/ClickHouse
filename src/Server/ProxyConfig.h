#pragma once

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#   include "config_core.h"
#endif

#include <common/types.h>

#include <boost/asio/ip/network_v4.hpp>
#include <boost/asio/ip/network_v6.hpp>

#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class ProxyProtocolHandler;

/// Interface class for configs for different proxy protocols.
class ProxyConfig {
public:
    explicit ProxyConfig(const std::string & name_, const std::string & protocol_);
    virtual ~ProxyConfig() = default;

    virtual std::unique_ptr<ProxyConfig> clone() const = 0;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config);
    virtual std::unique_ptr<ProxyProtocolHandler> createProxyProtocolHandler() const = 0;

public:
    using Network = std::variant<boost::asio::ip::network_v4, boost::asio::ip::network_v6>;
    using Networks = std::vector<Network>;

    const std::string name;
    const std::string protocol;
    Networks trusted_networks;
};

/// Config class for PROXY v1/v2 protocols.
class PROXYConfig final : public ProxyConfig
{
public:
    explicit PROXYConfig(const std::string & name_);

    virtual std::unique_ptr<ProxyConfig> clone() const override;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config) override;
    virtual std::unique_ptr<ProxyProtocolHandler> createProxyProtocolHandler() const override;

public:
    enum class Version { v1, v2 };

    Version version = Version::v1;
    bool allow_http_x_forwarded_for = false;
};

using ProxyConfigs = std::map<std::string, std::unique_ptr<ProxyConfig>>;

namespace Util
{

ProxyConfigs parseProxies(const Poco::Util::AbstractConfiguration & config);
ProxyConfigs clone(const ProxyConfigs & proxies);

}

}
