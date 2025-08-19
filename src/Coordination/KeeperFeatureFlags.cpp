#include <Coordination/KeeperFeatureFlags.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

std::pair<size_t, size_t> getByteAndBitIndex(size_t num)
{
    size_t byte_idx = num / 8;
    auto bit_idx = (7 - num % 8);
    return {byte_idx, bit_idx};
}

}

KeeperFeatureFlags::KeeperFeatureFlags()
{
    /// get byte idx of largest value
    auto [byte_idx, _] = getByteAndBitIndex(magic_enum::enum_count<KeeperFeatureFlag>() - 1);
    feature_flags = std::string(byte_idx + 1, 0);
}

KeeperFeatureFlags::KeeperFeatureFlags(std::string feature_flags_)
    : feature_flags(std::move(feature_flags_))
{}

void KeeperFeatureFlags::fromApiVersion(KeeperApiVersion keeper_api_version)
{
    if (keeper_api_version == KeeperApiVersion::ZOOKEEPER_COMPATIBLE)
        return;

    if (keeper_api_version >= KeeperApiVersion::WITH_FILTERED_LIST)
        enableFeatureFlag(KeeperFeatureFlag::FILTERED_LIST);

    if (keeper_api_version >= KeeperApiVersion::WITH_MULTI_READ)
        enableFeatureFlag(KeeperFeatureFlag::MULTI_READ);

    if (keeper_api_version >= KeeperApiVersion::WITH_CHECK_NOT_EXISTS)
        enableFeatureFlag(KeeperFeatureFlag::CHECK_NOT_EXISTS);
}

bool KeeperFeatureFlags::isEnabled(KeeperFeatureFlag feature_flag) const
{
    auto [byte_idx, bit_idx] = getByteAndBitIndex(magic_enum::enum_integer(feature_flag));

    if (byte_idx > feature_flags.size())
        return false;

    return feature_flags[byte_idx] & (1 << bit_idx);
}

void KeeperFeatureFlags::setFeatureFlags(std::string feature_flags_)
{
    feature_flags = std::move(feature_flags_);
}

void KeeperFeatureFlags::enableFeatureFlag(KeeperFeatureFlag feature_flag)
{
    auto [byte_idx, bit_idx] = getByteAndBitIndex(magic_enum::enum_integer(feature_flag));
    chassert(byte_idx < feature_flags.size());

    feature_flags[byte_idx] |= (1 << bit_idx);
}

void KeeperFeatureFlags::disableFeatureFlag(KeeperFeatureFlag feature_flag)
{
    auto [byte_idx, bit_idx] = getByteAndBitIndex(magic_enum::enum_integer(feature_flag));
    chassert(byte_idx < feature_flags.size());

    feature_flags[byte_idx] &= ~(1 << bit_idx);
}

const std::string & KeeperFeatureFlags::getFeatureFlags() const
{
    return feature_flags;
}

void KeeperFeatureFlags::logFlags(LoggerPtr log) const
{
    for (const auto & [feature_flag, feature_flag_name] : magic_enum::enum_entries<KeeperFeatureFlag>())
    {
        auto is_enabled = isEnabled(feature_flag);
        LOG_INFO(log, "Keeper feature flag {}: {}", feature_flag_name, is_enabled ? "enabled" : "disabled");
    }
}

}
