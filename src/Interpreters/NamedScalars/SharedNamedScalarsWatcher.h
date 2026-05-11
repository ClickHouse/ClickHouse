#pragma once

#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/NamedScalars/INamedScalarDefinitionStore.h>

#include <atomic>
#include <memory>

template <typename T> class ConcurrentBoundedQueue;

namespace DB
{

class NamedScalarsManager;

/// Reconciles the shared-scope scalar map with Keeper state.
/// Owned privately by NamedScalarsManager. See NamedScalarsManager.h
/// for the module overview.
///
/// Watch budget: 1 children-watch on the definitions directory + per-scalar
/// data-watches on `<root>/defs/<name>` and
/// `<root>/values/<uuid>/value`. Capped by
/// `<max_named_scalars>` (default 500).
///
/// Reconciliation paths:
///   * definitions children-fire (peer CREATE / DROP): re-read
///     getChildren, diff against the runtime, install fresh + drop missing.
///   * `<root>/defs/<name>` data-fire (peer OR REPLACE): targeted
///     reconcile for that scalar.
///
/// Value-side watches (`<root>/values/<uuid>/value`) do NOT go through
/// here: the backend invokes `NamedScalar::onValueChanged()` directly,
/// which is cheap enough to run on the Keeper IO thread (atomic flip +
/// task->schedule()).
class SharedNamedScalarsWatcher
{
public:
    SharedNamedScalarsWatcher(
        ContextPtr global_context_,
        WatchableNamedScalarDefinitionStorePtr definition_store_,
        NamedScalarsManager & manager_);
    ~SharedNamedScalarsWatcher();

    SharedNamedScalarsWatcher(const SharedNamedScalarsWatcher &) = delete;
    SharedNamedScalarsWatcher & operator=(const SharedNamedScalarsWatcher &) = delete;

    void start();
    void stop();

    /// Self-pickup nudge after a local CREATE / DROP, without waiting
    /// for the Keeper children-watch echo.
    void pokeResyncAll();

private:
    /// The queue carries no payload - it's just a wakeup mechanism for
    /// the watch thread. Real state lives in `resync_requested`.
    struct Wakeup {};

    static void requestResync(
        const std::shared_ptr<ConcurrentBoundedQueue<Wakeup>> & queue,
        const std::shared_ptr<std::atomic<bool>> & resync_requested);

    void watchLoop();
    void initialLoad();
    void resyncAll();
    void reconcileScalar(const String & name);

    Strings readDefinitionsAndInstallChildrenWatch();
    bool readDefinitionData(const String & name, String & out);

    ContextPtr global_context;
    WatchableNamedScalarDefinitionStorePtr definition_store;
    NamedScalarsManager & manager;
    LoggerPtr log;

    std::shared_ptr<ConcurrentBoundedQueue<Wakeup>> queue;
    std::shared_ptr<std::atomic<bool>> resync_requested;
    ThreadFromGlobalPool thread;
    std::atomic<bool> running{false};
    std::atomic<bool> loaded{false};
};

}
