#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Settings for [Zoo]Keeper-related works during BACKUP or RESTORE.
struct BackupKeeperSettings
{
    /// Maximum number of retries in the middle of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Should be big enough so the whole operation won't be cancelled in the middle of it because of a temporary ZooKeeper failure.
    UInt64 max_retries{1000};

    /// Initial backoff timeout for ZooKeeper operations during backup or restore.
    std::chrono::milliseconds retry_initial_backoff_ms{100};

    /// Max backoff timeout for ZooKeeper operations during backup or restore.
    std::chrono::milliseconds retry_max_backoff_ms{5000};

    /// If a host during BACKUP ON CLUSTER or RESTORE ON CLUSTER doesn't recreate its 'alive' node in ZooKeeper
    /// for this amount of time then the whole backup or restore is considered as failed.
    /// Should be bigger than any reasonable time for a host to reconnect to ZooKeeper after a failure.
    /// Set to zero to disable (if it's zero and some host crashed then BACKUP ON CLUSTER or RESTORE ON CLUSTER will be waiting
    /// for the crashed host for unlimited time until the operation is explicitly cancelled with KILL QUERY).
    std::chrono::seconds failure_after_host_disconnected_for_seconds{3600};

    /// How often the "stage" folder in ZooKeeper must be scanned in a background thread to track changes done by other hosts.
    std::chrono::milliseconds sync_period_ms{5000};

    /// The period of time during which all hosts participating in BACKUP ON CLUSTER or RESTORE ON CLUSTER should respond
    /// to the initiator of the query that they have started working on it.
    /// If some of the hosts don't respond for this period of time then the whole backup or restore is considered as failed.
    /// Set to zero to disable (this can make the initiator to wait for unlimited amount of time until the operation is
    /// explicitly cancelled with KILL QUERY).
    std::chrono::seconds on_cluster_initialization_timeout{180};

    /// Maximum number of retries during the initialization of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Shouldn't be too big because if the operation is going to fail then it's better if it fails faster.
    UInt64 max_retries_while_initializing{20};

    /// The period of time during which all hosts participating in BACKUP ON CLUSTER or RESTORE ON CLUSTER should react
    /// to the 'error' node appeared in the stage folder and finish their work.
    /// Shouldn't be too big because that timeout is just for cleanup after the operation has failed already.
    std::chrono::seconds on_cluster_error_handling_timeout{600};

    /// Maximum number of retries while handling an error of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
    /// Shouldn't be too big because those retries are just for cleanup after the operation has failed already.
    UInt64 max_retries_while_handling_error{50};

    /// Number of attempts after getting error ZBADVERSION from ZooKeeper.
    size_t max_attempts_after_bad_version{10};

    /// Maximum size of data of a ZooKeeper's node during backup.
    UInt64 value_max_size{1048576};

    /// Maximum size of a batch for a multi request.
    UInt64 batch_size_for_multi{1000};

    /// Maximum size of a batch for a multiread request.
    UInt64 batch_size_for_multiread{10000};

    /// Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f].
    Float64 fault_injection_probability{0};

    /// Seed for `fault_injection_probability`: 0 - random seed, otherwise the setting value.
    UInt64 fault_injection_seed{0};

    static BackupKeeperSettings fromContext(const ContextPtr & context);
};

}
