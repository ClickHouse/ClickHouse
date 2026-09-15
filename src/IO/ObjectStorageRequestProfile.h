#pragma once

#include <cstddef>
#include <cstdint>

namespace DB
{

/// Per-write retry-behavior selector, resolved by the object storage that executes the write.
/// SingleAttempt: exactly one HTTP attempt, no SDK-transparent retries — for conditional writes
/// whose retry loop lives above the storage client (it must resolve an uncertain PUT before
/// reissuing). Backends without a SingleAttempt implementation report it via
/// IObjectStorage::supportsRetryProfile; writers must fail closed rather than fall through.
enum class ObjectStorageRetryProfile : uint8_t
{
    Default,
    SingleAttempt,
};

/// A per-request override of retry behavior for an object storage call: which retry profile to use,
/// the per-attempt budget and connect cap the storage's single-attempt client must honour, and the
/// caller's own attempt number (0 = unset) so the HTTP client sees a reissue as attempt ≥ 2.
struct ObjectStorageControlRequest
{
    ObjectStorageRetryProfile profile = ObjectStorageRetryProfile::Default;
    uint64_t attempt_timeout_ms = 0;
    uint64_t connect_timeout_cap_ms = 0;
    size_t attempt_number = 0;
};

}
