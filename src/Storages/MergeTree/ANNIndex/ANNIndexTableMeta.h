#pragma once

#include <Core/Types.h>
#include <Disks/IVolume.h>

#include <memory>
#include <string_view>

namespace DB
{

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

/// Naming convention for per-group directories under `<ann-root>/`. Transitions between these
/// states are expressed as a single atomic `moveDirectory` — no separate catalog file is
/// involved. See `ANNIndexManager::loadFromDisk` for the on-startup classification logic.
///
/// Lifecycle:
///   `<ann-root>/tmp_ann_<uuid>/`      —  the current build is still writing here.
///   `<ann-root>/ann_<uuid>/`          —  active and visible to the query path.
///   `<ann-root>/deleting_ann_<uuid>/` —  retired; kept alive during a grace window so that
///                                        in-flight searchers holding a strong reference can
///                                        drain before the directory is recursively removed.
inline constexpr std::string_view ANN_GROUP_TMP_PREFIX      = "tmp_ann_";
inline constexpr std::string_view ANN_GROUP_ACTIVE_PREFIX   = "ann_";
inline constexpr std::string_view ANN_GROUP_DELETING_PREFIX = "deleting_ann_";

/// File name of the table-level metadata document (see `ANNIndexTableMeta`). It sits directly
/// under the ANN root, **not** inside any group directory. Each group has its own per-group
/// `meta.json` (with num_points / build_options / ...) that happens to share the same basename
/// but lives in a different directory.
inline constexpr std::string_view ANN_TABLE_META_FILE = "meta.json";

/// Shape fingerprint describing "what an index shaped like this one looks like":
///   - `dim`         — vector dimensionality
///   - `metric`      — distance metric id (matches `DiskANNMetric`: L2 = 0, Cosine = 1, ...)
///   - `algorithm`   — algorithm identifier (currently only `"diskann"`)
///   - `params_hash` — XXH64 over the sorted `k=v` algorithm parameter string
///
/// If the fingerprint computed from the current `CREATE INDEX` definition does not match the
/// one stored on disk, the manager must retire all existing groups and start fresh.
struct ANNIndexShapeFingerprint
{
    UInt32 dim = 0;
    UInt8 metric = 0;
    String algorithm;
    UInt64 params_hash = 0;

    bool operator==(const ANNIndexShapeFingerprint & rhs) const = default;
};

/// Representation of `<relative_root_path>/meta.json` — the **table-level** metadata.
///
/// JSON shape:
///   {
///     "version": 1,
///     "shape": {
///       "dim":         128,
///       "metric":      0,
///       "algorithm":   "diskann",
///       "params_hash": "0x..."       // hex string
///     },
///     "hash_algo":      "sipHash64",
///     "hash_seed":      "0x..."      // hex string
///   }
///
/// The document pins down the shape fingerprint (so a shape change at restart is detectable)
/// and the partition-id hash parameters (so coverage lookups remain consistent across runs).
/// The list of active / retired groups is derived at load time from the directory layout:
/// a directory name that starts with `ann_` is active, one starting with `deleting_ann_` is
/// retired. There is no separate catalog to keep in sync.
///
/// This artefact lives outside any group directory. Reads/writes go through `VolumePtr` +
/// `relative_root_path` directly rather than through `IANNGroupStorage` (which is per-group
/// by design).
struct ANNIndexTableMeta
{
    UInt32 version = 1;
    ANNIndexShapeFingerprint shape;
    String hash_algo = "sipHash64";
    UInt64 hash_seed = 0;

    /// Load `meta.json` from `<relative_root_path>/meta.json`. If the file does not exist an
    /// empty value is returned (detectable via `shape == ANNIndexShapeFingerprint{}`). Malformed
    /// JSON is reported as `CORRUPTED_DATA`.
    static ANNIndexTableMeta loadOrEmpty(VolumePtr volume, const std::string & relative_root_path);

    /// Persist the metadata document.
    ///
    /// - When `txn` is non-null all writes are routed through the transaction so that the
    ///   caller can commit them atomically together with the rest of the operation.
    /// - When `txn` is null the write is performed in two steps (`meta.json.tmp` + rename)
    ///   to survive a crash in the middle of the write.
    void writeTo(VolumePtr volume,
                 const std::string & relative_root_path,
                 DiskTransactionPtr txn = nullptr) const;
};

}
