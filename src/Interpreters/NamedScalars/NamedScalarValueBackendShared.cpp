#include <Interpreters/NamedScalars/NamedScalarValueBackendShared.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Interpreters/Context.h>
#include <base/getFQDNOrHostName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
}

namespace FailPoints
{
    extern const char shared_named_scalars_store_value_fail_once[];
}

namespace
{

zkutil::GetZooKeeper makeGetZooKeeper(const ContextPtr & context)
{
    std::weak_ptr<const Context> weak_ctx = context;
    return [weak_ctx]() -> zkutil::ZooKeeperPtr
    {
        auto locked = weak_ctx.lock();
        if (!locked)
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Global context for shared named_scalars value backend is gone");
        return locked->getZooKeeper();
    };
}

}

NamedScalarValueBackendShared::NamedScalarValueBackendShared(
    const ContextPtr & global_context_,
    const String & zookeeper_root_,
    std::function<void()> poke_resync_all_)
    : global_context(global_context_)
    , zookeeper_root(zookeeper_root_)
    , zookeeper_getter(makeGetZooKeeper(global_context_))
    , log(getLogger("NamedScalarValueBackendShared"))
    , poke_resync_all(std::move(poke_resync_all_))
{
    while (!zookeeper_root.empty() && zookeeper_root.back() == '/')
        zookeeper_root.pop_back();
    if (zookeeper_root.empty())
        zookeeper_root = "/";
}

zkutil::ZooKeeperPtr NamedScalarValueBackendShared::getZooKeeper()
{
    auto [zookeeper, session_status] = zookeeper_getter.getZooKeeper();
    if (session_status == zkutil::ZooKeeperCachingGetter::SessionStatus::New)
    {
        /// We may have reconnected to a different Keeper member; sync to
        /// guarantee read-your-writes after failover.
        zookeeper->sync(zookeeper_root);
        createRootNodesIfNeeded();
    }
    return zookeeper;
}

String NamedScalarValueBackendShared::valuesRootPath() const
{
    return childGroup("values");
}

String NamedScalarValueBackendShared::valueGroupPath(const String & value_key) const
{
    return valuesRootPath() + "/" + escapeForFileName(value_key);
}

String NamedScalarValueBackendShared::valuePath(const String & value_key) const
{
    return valueGroupPath(value_key) + "/value";
}

String NamedScalarValueBackendShared::lockPath(const String & value_key) const
{
    return valueGroupPath(value_key) + "/lock";
}

NamedScalarValueBackendShared::RefreshReservation::~RefreshReservation()
{
    if (!holder)
        return;
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::RefreshReservation::~RefreshReservation");
    holder.reset();
}

void NamedScalarValueBackendShared::createRootNodesIfNeeded()
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::createRootNodesIfNeeded");
    auto [zookeeper, _] = zookeeper_getter.getZooKeeper();
    zookeeper->createAncestors(zookeeper_root);
    zookeeper->createIfNotExists(zookeeper_root, "");
    zookeeper->createIfNotExists(valuesRootPath(), "");
}

std::optional<String> NamedScalarValueBackendShared::readValueBlob(const String & value_key)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::readValueBlob");
    auto zookeeper = getZooKeeper();
    const auto path = valuePath(value_key);
    String data;
    if (!zookeeper->tryGet(path, data))
        return std::nullopt;
    return data;
}

bool NamedScalarValueBackendShared::readValueWithDataWatch(
    const String & value_key,
    String & out,
    std::function<void()> on_change)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::readValueWithDataWatch");
    auto zookeeper = getZooKeeper();
    const String path = valuePath(value_key);

    auto watcher = zookeeper->createWatchFromRawCallback(
        fmt::format("NamedScalar(value/{})", value_key),
        [cb = std::move(on_change)]() -> Coordination::WatchCallback
        {
            return [cb](const Coordination::WatchResponse &) { cb(); };
        });

    Coordination::Stat stat;
    if (zookeeper->tryGetWatch(path, out, &stat, watcher))
        return true;
    /// Race between tryGetWatch and existsWatch: a peer can create the
    /// znode in between. Detect and re-read.
    if (zookeeper->existsWatch(path, &stat, watcher))
        return zookeeper->tryGetWatch(path, out, &stat, watcher);
    return false;
}

std::optional<String> NamedScalarValueBackendShared::readValueBlobAndWatch(
    const String & value_key,
    std::function<void()> on_change)
{
    String value_blob;
    /// `on_change` is `NamedScalar::onValueChanged` and is non-blocking
    /// (atomic flip + task->schedule()), so we can hand it to the Keeper
    /// raw-callback layer directly.
    if (!readValueWithDataWatch(value_key, value_blob, std::move(on_change)))
        return std::nullopt;
    return value_blob;
}

void NamedScalarValueBackendShared::removeValue(const String & value_key)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::removeValue");
    auto zookeeper = getZooKeeper();
    const auto path = valueGroupPath(value_key);
    auto code = zookeeper->tryRemoveRecursive(path);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw zkutil::KeeperException::fromPath(code, path);
}

std::unique_ptr<NamedScalarValueBackendShared::RefreshReservation> NamedScalarValueBackendShared::tryReserveRefresh(
    const String & value_key,
    const String & lock_holder_message)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::tryReserveRefresh");
    auto zookeeper = getZooKeeper();
    const String path = lockPath(value_key);
    zookeeper->createAncestors(path);
    auto holder = zkutil::EphemeralNodeHolder::tryCreate(
        path,
        *zookeeper,
        lock_holder_message.empty() ? getFQDNOrHostName() : lock_holder_message);
    if (!holder)
        return nullptr;

    auto reservation = std::make_unique<RefreshReservation>();
    reservation->zookeeper = zookeeper;
    reservation->holder = std::move(holder);
    reservation->path = path;

    Coordination::Stat value_stat;
    if (zookeeper->exists(valuePath(value_key), &value_stat))
        reservation->value_version = value_stat.version;
    return reservation;
}

RefreshPublishResult NamedScalarValueBackendShared::publishRefreshValue(
    const RefreshReservation & reservation,
    const String & name,
    const String & value_key,
    const String & payload)
{
    fiu_do_on(FailPoints::shared_named_scalars_store_value_fail_once,
        throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Injected failure while storing shared scalar '{}'", name););

    auto component_guard = Coordination::setCurrentComponent("NamedScalarValueBackendShared::publishRefreshValue");
    auto zookeeper = reservation.zookeeper;
    const auto path = valuePath(value_key);

    Coordination::Requests ops;
    if (reservation.value_version)
        ops.emplace_back(zkutil::makeSetRequest(path, payload, *reservation.value_version));
    else
        ops.emplace_back(zkutil::makeCreateRequest(path, payload, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeRemoveRequest(reservation.path, -1));

    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses, /* check_session_valid */ true);
    if (code == Coordination::Error::ZOK)
    {
        if (reservation.holder)
            reservation.holder->setAlreadyRemoved();
        return RefreshPublishResult::Published;
    }

    const auto release_code = zookeeper->tryRemove(reservation.path);
    if ((release_code == Coordination::Error::ZOK || release_code == Coordination::Error::ZNONODE) && reservation.holder)
        reservation.holder->setAlreadyRemoved();
    if (release_code != Coordination::Error::ZOK && release_code != Coordination::Error::ZNONODE)
        LOG_WARNING(
            log,
            "Could not release refresh lock for shared scalar '{}' after failed value publish: {}",
            name,
            Coordination::errorMessage(release_code));

    /// All of these mean "this scalar's local idea of who owns the slot
    /// no longer matches Keeper" — peer OR REPLACE, peer DROP, or our
    /// ephemeral lease was lost. Callers treat them identically.
    if (code == Coordination::Error::ZNONODE
        || code == Coordination::Error::ZBADVERSION
        || code == Coordination::Error::ZNODEEXISTS)
        return RefreshPublishResult::Diverged;
    throw zkutil::KeeperException::fromPath(code, path);
}

std::optional<NamedScalarRefreshLease> NamedScalarValueBackendShared::tryAcquireRefreshLease(
    const String & name,
    const String & value_key)
{
    auto reservation = tryReserveRefresh(value_key, getFQDNOrHostName());
    if (!reservation)
        return std::nullopt;

    auto acquired_reservation = std::shared_ptr<RefreshReservation>(std::move(reservation));
    return NamedScalarRefreshLease(
        [this, name, value_key, retained_reservation = std::move(acquired_reservation)](const String & value_blob)
        {
            const auto result = publishRefreshValue(
                *retained_reservation,
                name,
                value_key,
                value_blob);
            if (result != RefreshPublishResult::Published && poke_resync_all)
                poke_resync_all();
            return result;
        });
}

}
