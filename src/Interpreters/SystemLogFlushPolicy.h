#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <cstddef>
#include <cstdint>

namespace DB
{

struct BlockIO;

/// Interface that defines the flush behavior for system log tables.
class ISystemLogFlushPolicy
{
public:
    virtual ~ISystemLogFlushPolicy() = default;

    /// Returns `true` when the flush at `to_flush_end` corresponds to an explicit
    /// `SYSTEM FLUSH LOGS` command rather than a background periodic flush.
    virtual bool isManualFlush(uint64_t to_flush_end) = 0;

    /// Arms the engine for the next manual flush up to `target_index`.
    virtual void prepareManualFlush(uint64_t target_index) = 0;

    /// Called after the pipeline executor finishes writing `flush_size` rows.
    /// `is_manual_flush` is true when triggered by `SYSTEM FLUSH LOGS`.
    /// Implementations use this hook to track or wait for delivery.
    virtual void afterFlush(const BlockIO & io, bool is_manual_flush, size_t flush_size) = 0;

    /// Called when a flush fails with an exception before `afterFlush` could run.
    /// Implementations should reset any state set by `prepareManualFlush`.
    virtual void cancelManualFlush() {}

    /// Injects engine-specific settings into the query context used for `INSERT` queries.
    virtual void addInsertSettings(ContextMutablePtr & context) = 0;

    /// Returns `true` when alias columns must be omitted from `CREATE TABLE` queries.
    /// S3-backed engines do not support alias columns.
    virtual bool shouldSkipAliasColumns() = 0;
};

}
