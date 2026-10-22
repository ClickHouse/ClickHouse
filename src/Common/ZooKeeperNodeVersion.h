#pragma once

#include <Common/Exception.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace details
{

/// Represents data version of ZooKeeper Node.
/// Correctly handles single overflow to create a monotonic number abstraction
template <class Underlying = int64_t>
class ZooKeeperNodeVersionBase
{
protected:
    constexpr static int64_t kSmallest = std::numeric_limits<int64_t>::min();
    constexpr static int32_t kMinBound = std::numeric_limits<int32_t>::min();
    constexpr static int32_t kMaxBound = std::numeric_limits<int32_t>::max();
    constexpr static int32_t kCloseInterval = kMaxBound / 2;

    static int sign(int64_t value)
    {
        return (0 < value) - (value < 0);
    }

    static bool isClose(int64_t lhs, int64_t rhs)
    {
        int64_t delta = std::llabs(rhs - lhs);
        return delta <= kCloseInterval;
    }

    static bool isInBounds(int64_t value)
    {
        return kMinBound <= value && value <= kMaxBound;
    }

    static void checkBounds(int64_t value)
    {
        if (!isInBounds(value))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Inconsistent version of ZooKeeper Node. {} should be in bounds [{}, {}]",
                value, kMinBound, kMaxBound);
    }

public:
    ZooKeeperNodeVersionBase(int64_t version_ = kSmallest) /// NOLINT
        : version(version_)
    {
        if (version_ != kSmallest)
            checkBounds(version_);
    }

    bool operator<(const ZooKeeperNodeVersionBase & other) const noexcept
    {
        int64_t snapshot_lhs = version;
        int64_t snapshot_rhs = other.version;

        if (snapshot_lhs == kSmallest)
            return snapshot_lhs != snapshot_rhs;

        if (snapshot_rhs == kSmallest)
            return false;

        int sign_lhs = sign(snapshot_lhs);
        int sign_rhs = sign(snapshot_rhs);

        if (sign_lhs == sign_rhs)
            /// Monotonicity segment
            return snapshot_lhs < snapshot_rhs;

        if (isClose(snapshot_lhs, snapshot_rhs))
            return snapshot_lhs < snapshot_rhs;

        /// lhs > 0 and rhs < 0 then the rhs version overflowed
        return sign_lhs > sign_rhs;
    }

    bool operator==(const ZooKeeperNodeVersionBase & other) const noexcept { return version == other.version; }
    bool operator!=(const ZooKeeperNodeVersionBase & other) const noexcept { return !(*this == other); }
    bool operator<=(const ZooKeeperNodeVersionBase & other) const noexcept { return !(*this > other); }
    bool operator>(const ZooKeeperNodeVersionBase & other) const noexcept { return other < *this; }
    bool operator>=(const ZooKeeperNodeVersionBase & other) const noexcept { return !(*this < other); }

protected:
    Underlying version;
};

}

class ZooKeeperNodeVersion : public details::ZooKeeperNodeVersionBase<int64_t>
{
public:
    using ZooKeeperNodeVersionBase::ZooKeeperNodeVersionBase;

    int32_t toInt32() const;
    int64_t toInt64() const noexcept;

    bool isFromZooKeeper() const noexcept;
    bool isFakeVersion() const noexcept;
};

class AtomicZooKeeperNodeVersion : public details::ZooKeeperNodeVersionBase<std::atomic<int64_t>>
{
public:
    using ZooKeeperNodeVersionBase::ZooKeeperNodeVersionBase;

    ZooKeeperNodeVersion load() const noexcept;
    void store(ZooKeeperNodeVersion new_version);
    bool compareAndSet(ZooKeeperNodeVersion & expected, ZooKeeperNodeVersion new_version) noexcept;
};

}

template <>
struct fmt::formatter<DB::ZooKeeperNodeVersion>
{
    constexpr static auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::ZooKeeperNodeVersion & version, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", version.toInt64());
    }
};
