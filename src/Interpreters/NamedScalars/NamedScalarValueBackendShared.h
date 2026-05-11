#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/NamedScalars/INamedScalarValueBackend.h>

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <optional>

namespace zkutil
{
class EphemeralNodeHolder;
class ZooKeeper;
using EphemeralNodeHolderPtr = std::shared_ptr<EphemeralNodeHolder>;
using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class NamedScalarValueBackendShared final : public INamedScalarValueBackend
{
public:
    NamedScalarValueBackendShared(
        const ContextPtr & global_context_,
        const String & zookeeper_root_,
        std::function<void()> poke_resync_all_);

    std::optional<String> readValueBlob(const String & value_key) override;
    bool supportsValueWatches() const override { return true; }
    std::optional<String> readValueBlobAndWatch(const String & value_key, std::function<void()> on_change) override;
    void removeValue(const String & value_key) override;
    std::optional<NamedScalarRefreshLease> tryAcquireRefreshLease(
        const String & name,
        const String & value_key) override;

private:
    struct RefreshReservation
    {
        zkutil::ZooKeeperPtr zookeeper;
        zkutil::EphemeralNodeHolderPtr holder;
        String path;
        std::optional<Int32> value_version;

        /// EphemeralNodeHolder's destructor calls tryRemove on Keeper, which
        /// requires a current component to be set in this thread. Wrap the
        /// holder release in a guard so the cleanup path (lease dropped
        /// without publish, eval threw between acquire and publish, etc.)
        /// doesn't trip the LOGICAL_ERROR check.
        ~RefreshReservation();
    };

    /// Lazily creates `<root>/values`.
    void createRootNodesIfNeeded();

    RefreshPublishResult publishRefreshValue(
        const RefreshReservation & reservation,
        const String & name,
        const String & value_key,
        const String & payload);

    std::unique_ptr<RefreshReservation> tryReserveRefresh(
        const String & value_key,
        const String & lock_holder_message);

    bool readValueWithDataWatch(const String & value_key, String & out, std::function<void()> on_change);

    zkutil::ZooKeeperPtr getZooKeeper();

    /// `zookeeper_root` is either a non-empty path with no trailing
    /// slash, or "/". childGroup() collapses the latter so we never
    /// emit empty path components.
    String valuesRootPath() const;
    String valueGroupPath(const String & value_key) const;
    String valuePath(const String & value_key) const;
    String lockPath(const String & value_key) const;

    String childGroup(const String & child) const
    {
        return zookeeper_root == "/" ? "/" + child : zookeeper_root + "/" + child;
    }

    ContextPtr global_context;
    String zookeeper_root;
    zkutil::ZooKeeperCachingGetter zookeeper_getter;
    LoggerPtr log;
    std::function<void()> poke_resync_all;
};

}
