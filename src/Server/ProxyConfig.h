#pragma once

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#   include "config_core.h"
#endif

#include <common/types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

/// Interface class for configs for different proxy protocols.
class ProxyConfig {
public:
    explicit ProxyConfig(const std::string & name_, const std::string & protocol_);
    virtual ~ProxyConfig() = default;

    virtual std::unique_ptr<ProxyConfig> clone() const = 0;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config);

public:
    const std::string name;
    const std::string protocol;
};

/// Config class for PROXY v1/v2 protocols.
class PROXYConfig final : public ProxyConfig
{
public:
    explicit PROXYConfig(const std::string & name_);

    virtual std::unique_ptr<ProxyConfig> clone() const override;
    virtual void updateConfig(const Poco::Util::AbstractConfiguration & config) override;

public:
    enum class Version { v1, v2 };

    Version version = Version::v1;
    std::vector<std::string> nets;
    bool allow_http_x_forwarded_for = false;
};

namespace Util
{

std::map<std::string, std::unique_ptr<ProxyConfig>> parseProxies(
    const Poco::Util::AbstractConfiguration & config
);

}

}
