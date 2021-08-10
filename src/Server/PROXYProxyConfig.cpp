#include <Server/PROXYProxyConfig.h>
#include <Server/PROXYProxyProtocolHandler.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

PROXYProxyConfig::PROXYProxyConfig(const std::string & name_)
    : ProxyConfig(name_, "PROXY")
{
}

std::unique_ptr<ProxyConfig> PROXYProxyConfig::clone() const
{
    return std::make_unique<PROXYProxyConfig>(*this);
}

void PROXYProxyConfig::updateConfig(const Poco::Util::AbstractConfiguration & config)
{
    ProxyConfig::updateConfig(config);

    if (config.has("version"))
    {
        const auto version_num = config.getUInt("version");
        switch (version_num)
        {
            case 1: version = Version::v1; break;
            case 2: version = Version::v2; break;

            default:
                throw Exception("Bad PROXY protocol version " + std::to_string(version_num), ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }
}

std::unique_ptr<ProxyProtocolHandler> PROXYProxyConfig::createProxyProtocolHandler() const
{
    return std::make_unique<PROXYProxyProtocolHandler>(*this);
}

}
