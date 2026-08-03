#include <IO/S3/S3Capabilities.h>

#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

S3Capabilities::S3Capabilities(const S3Capabilities & src)
    : S3Capabilities(src.isBatchDeleteSupported(), src.support_proxy)
{
}

std::optional<bool> S3Capabilities::isBatchDeleteSupported() const
{
    std::lock_guard lock{mutex};
    return support_batch_delete;
}

void S3Capabilities::setIsBatchDeleteSupported(bool support_batch_delete_)
{
    std::lock_guard lock{mutex};

    if (support_batch_delete.has_value() && (support_batch_delete.value() != support_batch_delete_))
    {
        LOG_ERROR(getLogger("S3Capabilities"),
                  "Got different results ({} vs {}) from checking if the cloud storage supports batch delete (DeleteObjects), "
                  "the cloud storage API may be unstable",
                  support_batch_delete.value(), support_batch_delete_);
        chassert(false && "Got different results from checking if the cloud storage supports batch delete");
    }

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

void S3Capabilities::serialize(WriteBuffer & out) const
{
    int my_support_batch_delete;
    bool my_support_proxy;
    {
        std::lock_guard lock(mutex);
        if (support_batch_delete.has_value())
        {
            my_support_batch_delete = *support_batch_delete;
        }
        else
            my_support_batch_delete = 2;
        my_support_proxy = support_proxy;
    }
    writeBinary(my_support_batch_delete, out);
    writeBinary(my_support_proxy, out);
}

S3Capabilities S3Capabilities::deserialize(ReadBuffer & in)
{
    std::optional<bool> support_batch_deletes;
    int support_batch_deletes_int;
    readBinary(support_batch_deletes_int, in);
    if (support_batch_deletes_int != 2)
        support_batch_deletes = support_batch_deletes_int;

    bool my_support_proxy;
    readBinary(my_support_proxy, in);

    return S3Capabilities(support_batch_deletes, my_support_proxy);

}

}
