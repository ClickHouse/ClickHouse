#include <IO/S3/S3Capabilities.h>

namespace DB
{

S3Capabilities::S3Capabilities(const S3Capabilities & src)
    : S3Capabilities(src.is_batch_delete_supported(), src.support_proxy)
{
}

std::optional<bool> S3Capabilities::is_batch_delete_supported() const
{
    std::lock_guard lock{mutex};
    return support_batch_delete;
}

void S3Capabilities::set_is_batch_delete_supported(std::optional<bool> support_batch_delete_)
{
    std::lock_guard lock{mutex};
    support_batch_delete = support_batch_delete_;
}

S3Capabilities getCapabilitiesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    std::optional<bool> support_batch_delete;
    if (config.has(config_prefix + ".support_batch_delete"))
        support_batch_delete = config.getBool(config_prefix + ".support_batch_delete");

    bool support_proxy = config.getBool(config_prefix + ".support_proxy", config.has(config_prefix + ".proxy"));

    return S3Capabilities{support_batch_delete, support_proxy};
}

}
