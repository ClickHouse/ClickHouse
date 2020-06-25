#pragma once

/** Allows to compare two incremental counters of type UInt32 in presence of possible overflow.
  * We assume that we compare values that are not too far away.
  * For example, when we increment 0xFFFFFFFF, we get 0. So, 0xFFFFFFFF is less than 0.
  */
class WrappingUInt32
{
public:
    UInt32 value;

    explicit WrappingUInt32(UInt32 _value)
            : value(_value)
    {}

    bool operator<(const WrappingUInt32 & other) const
    {
        return value != other.value && *this <= other;
    }

    bool operator<=(const WrappingUInt32 & other) const
    {
        const UInt32 HALF = 1 << 31;
        return (value <= other.value && other.value - value < HALF)
               || (value > other.value && value - other.value > HALF);
    }

    bool operator==(const WrappingUInt32 & other) const
    {
        return value == other.value;
    }
};

/** Conforming Zxid definition.
  * cf. https://github.com/apache/zookeeper/blob/631d1b284f0edb1c4f6b0fb221bf2428aec71aaa/zookeeper-docs/src/main/resources/markdown/zookeeperInternals.md#guarantees-properties-and-definitions
  *
  * But it is better to read this: https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html
  *
  * Actually here is the definition of Zxid.
  * Every change to the ZooKeeper state receives a stamp in the form of a zxid (ZooKeeper Transaction Id).
  * This exposes the total ordering of all changes to ZooKeeper. Each change will have a unique zxid
  * and if zxid1 is smaller than zxid2 then zxid1 happened before zxid2.
  */
class Zxid
{
public:
    WrappingUInt32 epoch;
    WrappingUInt32 counter;
    explicit Zxid(UInt64 _zxid)
            : epoch(_zxid >> 32)
            , counter(_zxid)
    {}

    bool operator<=(const Zxid & other) const
    {
        return (epoch < other.epoch)
               || (epoch == other.epoch && counter <= other.counter);
    }

    bool operator==(const Zxid & other) const
    {
        return epoch == other.epoch && counter == other.counter;
    }
};

/* When multiple ClusterCopiers discover that the target partition is not empty,
 * they will attempt to clean up this partition before proceeding to copying.
 *
 * Instead of purging is_dirty, the history of cleaning work is preserved and partition hygiene is established
 * based on a happens-before relation between the events.
 * This relation is encoded by LogicalClock based on the mzxid of the is_dirty ZNode and is_dirty/cleaned.
 * The fact of the partition hygiene is encoded by CleanStateClock.
 *
 * For you to know what mzxid means:
 *
 * ZooKeeper Stat Structure:
 * The Stat structure for each znode in ZooKeeper is made up of the following fields:
 *
 * -- czxid
 * The zxid of the change that caused this znode to be created.
 *
 * -- mzxid
 * The zxid of the change that last modified this znode.
 *
 * -- ctime
 * The time in milliseconds from epoch when this znode was created.
 *
 * -- mtime
 * The time in milliseconds from epoch when this znode was last modified.
 *
 * -- version
 * The number of changes to the data of this znode.
 *
 * -- cversion
 * The number of changes to the children of this znode.
 *
 * -- aversion
 * The number of changes to the ACL of this znode.
 *
 * -- ephemeralOwner
 * The session id of the owner of this znode if the znode is an ephemeral node.
 * If it is not an ephemeral node, it will be zero.
 *
 * -- dataLength
 * The length of the data field of this znode.
 *
 * -- numChildren
 * The number of children of this znode.
 * */

class LogicalClock
{
public:
    std::optional<Zxid> zxid;

    LogicalClock() = default;

    explicit LogicalClock(UInt64 _zxid)
            : zxid(_zxid)
    {}

    bool hasHappened() const
    {
        return bool(zxid);
    }

    /// happens-before relation with a reasonable time bound
    bool happensBefore(const LogicalClock & other) const
    {
        return !zxid
               || (other.zxid && *zxid <= *other.zxid);
    }

    bool operator<=(const LogicalClock & other) const
    {
        return happensBefore(other);
    }

    /// strict equality check
    bool operator==(const LogicalClock & other) const
    {
        return zxid == other.zxid;
    }
};


class CleanStateClock
{
public:
    LogicalClock discovery_zxid;
    std::optional<UInt32> discovery_version;

    LogicalClock clean_state_zxid;
    std::optional<UInt32> clean_state_version;

    std::shared_ptr<std::atomic_bool> stale;

    bool is_clean() const
    {
        return !is_stale()
            && (!discovery_zxid.hasHappened() || (clean_state_zxid.hasHappened() && discovery_zxid <= clean_state_zxid));
    }

    bool is_stale() const
    {
        return stale->load();
    }

    CleanStateClock(
            const zkutil::ZooKeeperPtr & zookeeper,
            const String & discovery_path,
            const String & clean_state_path)
            : stale(std::make_shared<std::atomic_bool>(false))
    {
        Coordination::Stat stat{};
        String _some_data;
        auto watch_callback =
                [stale = stale] (const Coordination::WatchResponse & rsp)
                {
                    auto logger = &Poco::Logger::get("ClusterCopier");
                    if (rsp.error == Coordination::Error::ZOK)
                    {
                        switch (rsp.type)
                        {
                            case Coordination::CREATED:
                                LOG_DEBUG(logger, "CleanStateClock change: CREATED, at {}", rsp.path);
                                stale->store(true);
                                break;
                            case Coordination::CHANGED:
                                LOG_DEBUG(logger, "CleanStateClock change: CHANGED, at {}", rsp.path);
                                stale->store(true);
                        }
                    }
                };
        if (zookeeper->tryGetWatch(discovery_path, _some_data, &stat, watch_callback))
        {
            discovery_zxid = LogicalClock(stat.mzxid);
            discovery_version = stat.version;
        }
        if (zookeeper->tryGetWatch(clean_state_path, _some_data, &stat, watch_callback))
        {
            clean_state_zxid = LogicalClock(stat.mzxid);
            clean_state_version = stat.version;
        }
    }

    bool operator==(const CleanStateClock & other) const
    {
        return !is_stale()
               && !other.is_stale()
               && discovery_zxid == other.discovery_zxid
               && discovery_version == other.discovery_version
               && clean_state_zxid == other.clean_state_zxid
               && clean_state_version == other.clean_state_version;
    }

    bool operator!=(const CleanStateClock & other) const
    {
        return !(*this == other);
    }
};
