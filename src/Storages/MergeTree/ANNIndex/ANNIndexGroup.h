#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/ANNIndex/IANNIndexSearcher.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapReader.h>

#include <Core/Types.h>

#include <memory>
#include <vector>

namespace DB
{

/// Runtime handle for a fully-built ANN index group.
///
/// A group bundles four artefacts produced together by the ANN index build pipeline:
///   - an algorithm-specific searcher behind the `IANNIndexSearcher` interface;
///   - a `PartRowIdMapReader` loaded once from `id_map.bin`;
///   - an `ANNGroupCoverage` loaded once from `coverage.bin`;
///   - a shape fingerprint and `hash_seed` read back from `meta.json`.
///
/// The object is immutable after construction: all members (in particular the searcher) are
/// safe to use concurrently from multiple threads without additional synchronisation.
class ANNIndexGroup
{
public:
    using SearchHit = ANNSearcherHit;

    /// Reload a previously-built group from disk. Reads `meta.json` to obtain the shape and
    /// hash seed, opens the searcher via `createANNIndexSearcher` using `defaults` for the
    /// algorithm-specific tuning knobs, and mmaps the id_map / coverage sidecars.
    ///
    /// Throws `CORRUPTED_DATA` if any sidecar is missing, corrupt, or inconsistent with
    /// `meta.json` (e.g. id_map row count does not match `num_points`).
    static std::shared_ptr<ANNIndexGroup> load(
        ANNGroupStoragePtr storage,
        ANNSearchDefaultsPtr defaults);

    /// Directly construct from already-built components. Used by the builder to avoid
    /// reopening the searcher and re-loading the sidecars immediately after `build`. Callers
    /// must guarantee consistency of the triplet (same `num_points`, same shape).
    /// `search_defaults_` is retained so that `rebindStorage` can recreate the searcher after
    /// a directory rename without requiring the caller to re-supply it.
    ANNIndexGroup(
        ANNGroupStoragePtr storage_,
        ANNIndexShapeFingerprint shape_,
        UInt64 hash_seed_,
        ANNSearchDefaultsPtr search_defaults_,
        IANNIndexSearcherPtr searcher_,
        PartRowIdMapReader id_map_,
        ANNGroupCoverage coverage_);

    virtual ~ANNIndexGroup() = default;
    ANNIndexGroup(const ANNIndexGroup &) = delete;
    ANNIndexGroup & operator=(const ANNIndexGroup &) = delete;

    /// Run an ANN search over the group. `search_list_size` / `beam_width` may be overridden
    /// per query through `ANNSearchOverrides` (zero leaves the construction-time default
    /// untouched); other algorithm-specific tuning remains baked into the searcher.
    virtual std::vector<SearchHit> search(
        const float * query,
        size_t query_dim,
        size_t k,
        const ANNSearchOverrides & overrides) const;

    /// Map a DiskANN `internal_id` (i.e. vertex id) back to the source row identity.
    /// Unchecked — caller must ensure `internal_id < numPoints()`.
    virtual PartRowId lookup(UInt32 internal_id) const { return id_map.lookup(internal_id); }

    /// Coverage predicate exposed for the manager's `isPartCovered` check.
    virtual bool containsPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const
    {
        return coverage.containsPart(partition_hash, min_block, max_block);
    }

    virtual const ANNGroupCoverage & getCoverage() const { return coverage; }
    virtual const ANNIndexShapeFingerprint & getShape() const { return shape; }
    virtual UInt64 getHashSeed() const { return hash_seed; }
    virtual size_t numPoints() const { return id_map.size(); }

    /// Borrow the underlying searcher, primarily so that callers outside the `search`
    /// hot path can reach the algorithm's stateless distance kernel via
    /// `IANNIndexSearcher::computeDistances` without going back through the factory.
    /// The searcher is immutable and safe to share across threads.
    virtual IANNIndexSearcherPtr getSearcher() const { return searcher; }

    /// Last path component of the group directory, e.g. `ann_<uuid>`.
    virtual std::string getGroupDir() const { return storage->getGroupDir(); }
    const IANNGroupStorage & getStorage() const { return *storage; }

    /// Replace the group storage handle and reopen the searcher against the new directory.
    /// Used after a build that constructed the group against a temporary directory
    /// (`tmp_ann_<uuid>`) and then committed a rename to the active directory (`ann_<uuid>`),
    /// or after a rename to a retired directory (`deleting_ann_<uuid>`). Whether re-opening is
    /// physically required depends on the algorithm — e.g. the DiskANN FFI opens
    /// `idx_disk.index` lazily on every search by path string, so a rename invalidates the
    /// cached path. This is handled inside the concrete searcher returned by
    /// `createANNIndexSearcher`; the id_map / coverage sidecars are already in memory and are
    /// not reloaded.
    virtual void rebindStorage(ANNGroupStoragePtr new_storage);

    /// `meta.json` file name used by the builder / loader pair.
    static constexpr std::string_view META_FILE_NAME = "meta.json";

protected:
    /// Tag type that enables a derived class to construct an `ANNIndexGroup` without opening
    /// the FFI searcher. This is strictly for unit tests that want to override the virtual
    /// interface with preset in-memory state; production code must use the public ctor or
    /// `load`. The base class keeps `storage` / `searcher` as null pointers; any virtual
    /// method that would otherwise dereference them must be overridden by the derived class.
    struct TestOnlyTag
    {
        explicit TestOnlyTag() = default;
    };

    ANNIndexGroup(
        TestOnlyTag,
        ANNIndexShapeFingerprint shape_,
        UInt64 hash_seed_,
        ANNGroupCoverage coverage_);

private:
    ANNGroupStoragePtr storage;
    ANNIndexShapeFingerprint shape;
    UInt64 hash_seed;
    ANNSearchDefaultsPtr search_defaults;
    IANNIndexSearcherPtr searcher;
    PartRowIdMapReader id_map;
    ANNGroupCoverage coverage;
};

using ANNIndexGroupPtr = std::shared_ptr<ANNIndexGroup>;

}
#endif
