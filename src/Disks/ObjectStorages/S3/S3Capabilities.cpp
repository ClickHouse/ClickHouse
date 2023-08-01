#include <Disks/ObjectStorages/S3/S3Capabilities.h>

namespace DB
{

S3Capabilities getCapabilitiesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return S3Capabilities
    {
        .support_batch_delete = config.getBool(config_prefix + ".support_batch_delete", true),
        .support_proxy = config.getBool(config_prefix + ".support_proxy", config.has(config_prefix + ".proxy")),
    };
}

}
