#pragma once

/// Shared test doubles for the txn gtests. One coordinator/store/registry
/// family drives both the read-path (snapshot) and commit-driver tests:
///
///   * `FakeCoordinator` — bump-on-commit semantics (`attemptCommit` assigns
///     `csn + 1`); `csn` is a public field, so read-path tests set the
///     snapshot point directly. An optional `drift_sampler` lets the assigned
///     csn exceed `snap.csn + 1`. `withinSnapshotRegion` invokes `fn(csn)`
///     directly (no real lock — tests are single-threaded around capture).
///   * `RecordingBitmapStore` — serves the max version ≤ snapshot_csn and
///     records every `installBitmap`/`removeBitmap` (and, optionally, enforces
///     the disk-derived monotonicity invariant).
///   * `CountingPinRegistry` — tracks live pins per csn for pin-RAII / `total()`
///     assertions; `clusterFloor()` is the real min-pinned-csn (MAX_CSN when
///     none).
///
/// `makeFixture()` wires them into a real `PartitionTxnController` (null data →
/// the snapshot capture sees an empty parts list) and returns raw pointers to
/// all three plus the controller.

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/ICommitCoordinator.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <Common/Exception.h>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

namespace DB::UniqueKeyTxn::tests
{

/// Counting pin registry: tracks live pin handles per csn so tests can assert
/// pin RAII behaviour without depending on `LocalPinRegistry`'s internals.
class CountingPinRegistry : public IPinRegistry
{
public:
    std::map<CSN, UInt64> pins_by_csn;

    std::shared_ptr<PinHandle> acquire(CSN csn) override
    {
        ++pins_by_csn[csn];
        return makeHandle(csn, [this, csn]
        {
            auto it = pins_by_csn.find(csn);
            if (it != pins_by_csn.end() && --it->second == 0)
                pins_by_csn.erase(it);
        });
    }
    CSN clusterFloor() override
    {
        return pins_by_csn.empty() ? MAX_CSN : pins_by_csn.begin()->first;
    }

    UInt64 total() const
    {
        UInt64 t = 0;
        for (const auto & p : pins_by_csn)
            t += p.second;
        return t;
    }
};

inline DeleteBitmapPtr makeBitmap(std::initializer_list<UInt64> values)
{
    auto bm = std::make_shared<DeleteBitmap>();
    for (auto v : values)
        bm->add(v);
    return bm;
}

/// Sink-the-calls bitmap store. Records every `installBitmap(target, csn,
/// bytes)` and `removeBitmap(target, csn)` so a test can assert exactly which
/// sidecar each commit wrote and which the rollback undid. Serves `readBitmap`
/// from the recorded set (max version ≤ snapshot).
///
/// `cached_version[target]` mirrors the production fetch-max cached bitmap
/// version (`IMergeTreeDataPart::getCachedCurrentBitmapVersion`); `removeBitmap`
/// resets it to the next-highest version still in `store` for that target (or 0)
/// — the cleanest rollback outcome a bitmap store could offer for the
/// per-part cache. Readers in the production tree do NOT consult this cache for
/// bitmap selection (they enumerate the dir + `pickHighest`), so the model is
/// purely a test-fixture honesty concern.
///
/// `enforce_monotonicity` (off by default) makes `installBitmap` throw
/// LOGICAL_ERROR when a csn ≤ a prior installed version for the target — mirrors
/// `IBitmapStore::installBitmap`'s contract for the cumulative-publish tests.
class RecordingBitmapStore : public IBitmapStore
{
public:
    struct PutCall
    {
        PartName target;
        CSN csn = INVALID_CSN;
        DeleteBitmapPtr bytes;
    };
    std::vector<PutCall> puts;

    struct UnlinkCall
    {
        PartName target;
        CSN csn = INVALID_CSN;
    };
    std::vector<UnlinkCall> unlinks;

    std::map<std::pair<PartName, CSN>, DeleteBitmapPtr> store;
    std::map<PartName, UInt64> cached_version;

    bool enforce_monotonicity = false;

    void seed(const PartName & target, CSN csn, DeleteBitmapPtr bytes)
    {
        store[{target, csn}] = std::move(bytes);
        cached_version[target] = std::max(csn, cached_version[target]);
    }

    std::pair<ConstDeleteBitmapPtr, CSN>
    readBitmap(const PartName & part, CSN snapshot_csn) override
    {
        std::optional<std::pair<CSN, DeleteBitmapPtr>> chosen;
        for (auto & [k, v] : store)
        {
            if (k.first != part || k.second > snapshot_csn)
                continue;
            if (!chosen || k.second > chosen->first)
                chosen = {k.second, v};
        }
        if (!chosen)
            return {std::make_shared<DeleteBitmap>(), 0};  /// empty non-null on miss
        return {chosen->second, chosen->first};
    }

    void installBitmap(const PartName & target, CSN csn, const DeleteBitmap & bitmap) override
    {
        if (enforce_monotonicity)
        {
            auto it = cached_version.find(target);
            if (it != cached_version.end() && csn <= it->second)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                    "RecordingBitmapStore::installBitmap non-monotone csn");
        }
        auto owned = std::make_shared<DeleteBitmap>();
        owned->addMany(bitmap.toVector());
        puts.push_back({target, csn, owned});
        store[{target, csn}] = std::move(owned);
        cached_version[target] = std::max(csn, cached_version[target]);
    }

    void removeBitmap(const PartName & target, CSN csn) override
    {
        unlinks.push_back({target, csn});
        store.erase({target, csn});

        UInt64 next_highest = 0;
        for (const auto & [k, _] : store)
        {
            if (k.first == target && k.second > next_highest)
                next_highest = k.second;
        }
        cached_version[target] = next_highest;
    }
};

/// `removeBitmap` always throws (a non-LOGICAL_ERROR code so the debug build
/// doesn't abort) — drives the commit-rollback double failure (publish throws,
/// then rollback cannot undo the install) that latches the partition fail-closed.
class ThrowOnRemoveBitmapStore : public RecordingBitmapStore
{
public:
    void removeBitmap(const PartName &, CSN) override
    {
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "injected removeBitmap failure");
    }
};

/// `installBitmap` durably "writes" (records the put) and THEN throws — models
/// `MergeTreeBitmapStore::installBitmap` throwing after its write+rename (the
/// post-write re-enumeration failure), leaving the sidecar on disk. `removeBitmap`
/// works, so the commit's rollback can undo the orphan.
class InstallThrowsAfterWriteStore : public RecordingBitmapStore
{
public:
    void installBitmap(const PartName & target, CSN csn, const DeleteBitmap & bitmap) override
    {
        RecordingBitmapStore::installBitmap(target, csn, bitmap);  /// durable write (records the put)
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "injected post-write installBitmap failure");
    }
};

/// Bump-on-commit coordinator. `attemptCommit` allocates `csn + 1` (after an
/// optional `drift_sampler` bump that mirrors `LocalCommitCoordinator`'s
/// under-lock `max(info.max_block)` sample, so the `assigned_csn` handed to
/// staging can exceed `snap.csn + 1`). Throw-safety: a throw inside staging
/// propagates without advancing `csn` (recovery owns orphan cleanup).
class FakeCoordinator : public ICommitCoordinator
{
public:
    CSN csn = INVALID_CSN;
    std::function<CSN()> drift_sampler;

    PreparedCommitSnapshot readSnapshot() override
    {
        PreparedCommitSnapshot s;
        s.csn = csn;
        return s;
    }

    CSN attemptCommit(PublishAction staging) override
    {
        if (drift_sampler)
        {
            const CSN sampled = drift_sampler();
            csn = std::max(sampled, csn);
        }
        const CSN tentative = csn + 1;
        if (staging)
            staging(tentative);
        csn = tentative;
        return tentative;
    }

    void withinSnapshotRegion(std::function<void(CSN)> fn) override { fn(csn); }

    /// Tests drive the controller single-threaded; an empty guard suffices.
    std::unique_lock<std::mutex> lockForWrite() override { return {}; }
};

struct Fixture
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    CountingPinRegistry * pins = nullptr;
    std::unique_ptr<PartitionTxnController> state;
};

inline Fixture makeFixture()
{
    auto store_owned = std::make_unique<RecordingBitmapStore>();
    auto coord_owned = std::make_unique<FakeCoordinator>();
    auto pin_owned = std::make_unique<CountingPinRegistry>();
    Fixture fx;
    fx.store = store_owned.get();
    fx.coord = coord_owned.get();
    fx.pins = pin_owned.get();
    fx.state = std::make_unique<PartitionTxnController>(
        std::move(coord_owned),
        std::move(store_owned),
        std::move(pin_owned));
    return fx;
}

}
