#include <Common/ZooKeeperNodeVersion.h>

namespace DB
{

int32_t ZooKeeperNodeVersion::toInt32() const
{
    checkBounds(version);
    return static_cast<int32_t>(version);
}

int64_t ZooKeeperNodeVersion::toInt64() const noexcept
{
    return version;
}

bool ZooKeeperNodeVersion::isFromZooKeeper() const noexcept
{
    return isInBounds(version);
}

bool ZooKeeperNodeVersion::isFakeVersion() const noexcept
{
    return !isFromZooKeeper();
}

ZooKeeperNodeVersion AtomicZooKeeperNodeVersion::load() const noexcept
{
    return ZooKeeperNodeVersion(version.load());
}

void AtomicZooKeeperNodeVersion::store(ZooKeeperNodeVersion new_version)
{
    if (new_version.toInt64() != kSmallest)
        checkBounds(new_version.toInt64());

    version.store(new_version.toInt64());
}

bool AtomicZooKeeperNodeVersion::compareAndSet(ZooKeeperNodeVersion & expected, ZooKeeperNodeVersion new_version) noexcept
{
    int64_t expected_underlying = expected.toInt64();

    bool success = version.compare_exchange_strong(expected_underlying, new_version.toInt64());
    expected = ZooKeeperNodeVersion(expected_underlying);

    return success;
}

}
