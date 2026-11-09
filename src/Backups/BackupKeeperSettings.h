#pragma once

#include <Interpreters/Context_fwd.h>
#include <chrono>


namespace DB
{

/// Settings for [Zoo]Keeper-related works during BACKUP or RESTORE.
struct BackupKeeperSettings
{
    explicit BackupKeeperSettings(const ContextPtr & context);

    /// Maximum number of retries in the middle of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Should be big enough so the whole operation won't be cancelled in the middle of it because of a temporary ZooKeeper failure.
    UInt64 max_retries;

    /// Initial backoff timeout for ZooKeeper operations during backup or restore.
    std::chrono::milliseconds retry_initial_backoff_ms;

    /// Max backoff timeout for ZooKeeper operations during backup or restore.
    std::chrono::milliseconds retry_max_backoff_ms;

    /// If a host during BACKUP ON CLUSTER or RESTORE ON CLUSTER doesn't recreate its 'alive' node in ZooKeeper
    /// for this amount of time then the whole backup or restore is considered as failed.
    /// Should be bigger than any reasonable time for a host to reconnect to ZooKeeper after a failure.
    /// Set to zero to disable (if it's zero and some host crashed then BACKUP ON CLUSTER or RESTORE ON CLUSTER will be waiting
    /// for the crashed host forever until the operation is explicitly cancelled with KILL QUERY).
    std::chrono::seconds failure_after_host_disconnected_for_seconds;

    /// Maximum number of retries during the initialization of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Shouldn't be too big because if the operation is going to fail then it's better if it fails faster.
    UInt64 max_retries_while_initializing;

    /// Maximum number of retries while handling an error of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Shouldn't be too big because those retries are just for cleanup after the operation has failed already.
    UInt64 max_retries_while_handling_error;

    /// How long the initiator should wait for other host to handle the 'error' node and finish their work.
    std::chrono::seconds finish_timeout_after_error;

    /// How often the "stage" folder in ZooKeeper must be scanned in a background thread to track changes done by other hosts.
    std::chrono::milliseconds sync_period_ms;

    /// Number of attempts after getting error ZBADVERSION from ZooKeeper.
    size_t max_attempts_after_bad_version;

    /// Maximum size of data of a ZooKeeper's node during backup.
    UInt64 value_max_size;

    /// Maximum size of a batch for a multi request.
    UInt64 batch_size_for_multi;

    /// Maximum size of a batch for a multiread request.
    UInt64 batch_size_for_multiread;

    /// Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f].
    Float64 fault_injection_probability;

    /// Seed for `fault_injection_probability`: 0 - random seed, otherwise the setting value.
    UInt64 fault_injection_seed;
};

}
