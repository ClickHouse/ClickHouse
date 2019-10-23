#include <Interpreters/IExternalLoadable.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <cmath>

namespace DB
{

ExternalLoadableLifetime::ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config,
                                                   const std::string & config_prefix)
{
    const auto & lifetime_min_key = config_prefix + ".min";
    const auto has_min = config.has(lifetime_min_key);

    min_sec = has_min ? config.getUInt64(lifetime_min_key) : config.getUInt64(config_prefix);
    max_sec = has_min ? config.getUInt64(config_prefix + ".max") : min_sec;
}


UInt64 calculateDurationWithBackoff(pcg64 & rnd_engine, size_t error_count)
{
    constexpr UInt64 backoff_initial_sec = 5;
    constexpr UInt64 backoff_max_sec = 10 * 60; /// 10 minutes

    if (error_count < 1)
        error_count = 1;

    /// max seconds is 600 and 2 ** 10 == 1024
    if (error_count > 11)
        error_count = 11;

    std::uniform_int_distribution<UInt64> distribution(0, static_cast<UInt64>(std::exp2(error_count - 1)));
    return std::min<UInt64>(backoff_max_sec, backoff_initial_sec + distribution(rnd_engine));
}

}
