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
    constexpr UInt64 backoff_max_iterations = 9; /// 2^9 = 512 seconds

    if (error_count > backoff_max_iterations)
        error_count = backoff_max_iterations;

    std::uniform_int_distribution<UInt64> distribution(0, static_cast<UInt64>(std::exp2(error_count)));
    return distribution(rnd_engine);
}

}
