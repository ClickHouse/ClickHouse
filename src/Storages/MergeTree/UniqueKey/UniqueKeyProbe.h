#pragma once

#include <base/types.h>
#include <Core/Names.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
class Block;

/// Selects the probe implementation; defined in `Core/SettingsEnums.h` and
/// surfaced by the `unique_key_probe_implementation` setting. Forward-declared
/// here (matching its `uint8_t` underlying type) so the contract header stays
/// free of the settings include.
enum class UniqueKeyProbeImplementation : uint8_t;

/// UNIQUE KEY — write-path probe interface.
///
/// The probe answers, for a batch of keys: "does this key already live in this
/// partition, and if so, where?" The caller (the sink) supplies an active-parts
/// snapshot that must stay stable for the duration of the call.
///
/// Three outcomes per key, keyed by "newest-first among active parts":
///   `NOT_FOUND`       — no active part contains the key (caller inserts fresh).
///   `FOUND_LIVE`      — a part has the key with a live bitmap bit; `part` /
///                       `row_number` point to the newest such row.
///   `FOUND_ALL_DEAD`  — the key exists but every occurrence is bitmap-dead.
enum class ProbeOutcome : UInt8
{
    NOT_FOUND = 0,
    FOUND_LIVE = 1,
    FOUND_ALL_DEAD = 2,
};

struct ProbeResult
{
    ProbeOutcome outcome = ProbeOutcome::NOT_FOUND;

    /// When `outcome == FOUND_LIVE`: the part that owns the live row, else
    /// nullptr. Non-owning; lifetime bound to the parts snapshot the caller
    /// passed in.
    const IMergeTreeDataPart * part = nullptr;

    /// Row index inside `part` (== `_part_offset`) when `FOUND_LIVE`. `UInt64`
    /// to match `_part_offset`'s type.
    UInt64 row_number = 0;
};

/// Minimal abstraction over "a part that might own the key". The probe driver
/// encodes the key batch once (via `UniqueKeyEncoding`) and hands each target
/// the encoded keys; the target never re-encodes. The driver traverses a
/// snapshot of these targets newest-first and stops at the first live match
/// per key.
class IProbeTargetPart
{
public:
    virtual ~IProbeTargetPart() = default;

    /// For each `encoded_keys[i]`, set `out[i]` to the `_part_offset` of a
    /// matching row in this part, or `std::nullopt` if absent. `out` is resized
    /// to `encoded_keys.size()`. The driver then checks the live bitmap.
    virtual void findRowIndexBatch(
        const std::vector<std::string_view> & encoded_keys,
        std::vector<std::optional<UInt64>> & out) const = 0;

    /// Whether the row at `row_number` is bitmap-dead in this part.
    virtual bool isRowDead(UInt64 row_number) const = 0;

    /// The underlying part, surfaced through `ProbeResult::part`. Test fakes
    /// return nullptr; production targets return the real part pointer.
    virtual const IMergeTreeDataPart * getUnderlyingPart() const = 0;
};

using ProbeTargetPartPtr = std::shared_ptr<const IProbeTargetPart>;

/// A snapshot of probe targets for one partition, ordered newest-first.
using ProbeTargetsSnapshot = std::vector<ProbeTargetPartPtr>;

/// Supplies the newest-first parts snapshot for a partition, called at the
/// start of each probe.
using ProbeTargetsSupplier = std::function<ProbeTargetsSnapshot(const String & partition_id)>;

/// Abstract probe interface. Engine-agnostic; the MergeTree concrete is
/// `UniqueKeyProbeSimple`. Construct via `makeUniqueKeyProbe`.
class IUniqueKeyProbe
{
public:
    virtual ~IUniqueKeyProbe() = default;

    /// Probe a batch of keys. `keys` holds the unique-key columns (in unique-key
    /// column order); returns one `ProbeResult` per row, aligned with `keys`.
    virtual std::vector<ProbeResult> probeBatch(const Block & keys, const String & partition_id) = 0;
};

using UniqueKeyProbePtr = std::unique_ptr<IUniqueKeyProbe>;

/// Construct the `IUniqueKeyProbe` selected by the `unique_key_probe_implementation`
/// setting. Today only the simple (single-threaded) implementation exists, so
/// `Auto` and `Simple` both build a `UniqueKeyProbeSimple`. The switch is the
/// seam a future parallel implementation plugs into without changing call sites.
UniqueKeyProbePtr makeUniqueKeyProbe(
    UniqueKeyProbeImplementation impl,
    ProbeTargetsSupplier supplier,
    Names unique_key_column_names,
    size_t max_encoded_size);

}
