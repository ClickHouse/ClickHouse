#pragma once

#include <Backups/BackupConcurrencyCheck.h>
#include <Backups/WithRetries.h>
#include <Common/threadPoolCallbackRunner.h>


namespace DB
{

/// Used to coordinate hosts so all hosts would come to a specific stage at around the same time.
class BackupCoordinationStageSync
{
public:
    /// Empty string as the current host is used to mark the initiator of a BACKUP ON CLUSTER or RESTORE ON CLUSTER query.
    static const constexpr std::string_view kInitiator;

    BackupCoordinationStageSync(
        bool is_restore_,                    /// true if this is a RESTORE ON CLUSTER command, false if this is a BACKUP ON CLUSTER command
        const String & zookeeper_path_,      /// path to the "stage" folder in ZooKeeper
        const String & current_host_,        /// the current host, or an empty string if it's the initiator of the BACKUP/RESTORE ON CLUSTER command
        const Strings & all_hosts_,          /// all the hosts (including the initiator and the current host) performing the BACKUP/RESTORE ON CLUSTER command
        bool allow_concurrency_,             /// whether it's allowed to have concurrent backups or restores.
        BackupConcurrencyCounters & concurrency_counters_,
        const WithRetries & with_retries_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_,
        QueryStatusPtr process_list_element_,
        LoggerPtr log_);

    ~BackupCoordinationStageSync();

    void startup();

    /// Sets that the BACKUP or RESTORE query was sent to other hosts.
    void setQueryIsSentToOtherHosts();
    bool isQuerySentToOtherHosts() const;

    /// Sets the stage of the current host and signal other hosts if there were other hosts waiting for that.
    void setStage(const String & stage, const String & stage_result = {});

    /// Waits until specified hosts come to the specified stage.
    /// The function returns the results which the specified hosts set when they came to the required stage.
    Strings waitHostsReachStage(const Strings & hosts, const String & stage_to_wait) const;

    /// Lets other hosts know that the current host has encountered an error.
    /// The function returns true if it successfully created the error node or if the error node was found already exist.
    void setError(std::exception_ptr exception, bool throw_if_error);
    bool isErrorSet() const;

    /// Waits until the hosts other than the current host finish their work. Must be called before finish().
    /// Stops waiting and throws an exception if another host encounters an error or if some host gets cancelled.
    void waitOtherHostsFinish(bool throw_if_error) const;
    bool otherHostsFinished() const;

    /// Lets other hosts know that the current host has finished its work.
    void finish(bool throw_if_error);
    bool finished() const;

    /// Returns true if all the hosts have finished.
    bool allHostsFinished() const { return finished() && otherHostsFinished(); }

    /// Returns a list of the hosts which haven't finished yet.
    Strings getUnfinishedHosts() const;
    Strings getUnfinishedOtherHosts() const;

    /// Returns a printable name of a specific host. For empty host the function returns "initiator".
    static String getHostDesc(const String & host);
    static String getHostsDesc(const Strings & hosts);

private:
    /// Initializes the original state. It will be updated then with readCurrentState().
    void initializeState();

    /// Creates the root node in ZooKeeper.
    void createRootNodes();

    /// Atomically creates both 'start' and 'alive' nodes and also checks that there is no concurrent backup or restore if `allow_concurrency` is false.
    void createStartAndAliveNodesAndCheckConcurrency();
    void createStartAndAliveNodesAndCheckConcurrency(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);

    /// Deserialize the version of a node stored in the 'start' node.
    int parseStartNode(const String & start_node_contents, const String & host) const;

    /// Recreates the 'alive' node if it doesn't exist. It's an ephemeral node so it's removed automatically after disconnections.
    void createAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);

    /// Checks that there is no concurrent backup or restore if `allow_concurrency` is false.
    void checkConcurrency(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);

    /// Watching thread periodically reads the current state from ZooKeeper and recreates the 'alive' node.
    void startWatchingThread();
    void stopWatchingThread();
    void watchingThread();

    /// Reads the current state from ZooKeeper without throwing exceptions.
    void readCurrentState(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);

    /// Creates a stage node to let other hosts know we've reached the specified stage.
    void createStageNode(const String & stage, const String & stage_result, Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);
    String getStageNodePath(const String & stage) const;

    /// Lets other hosts know that the current host has encountered an error.
    void setError(const Exception & exception, bool throw_if_error);
    [[noreturn]] void rethrowSetError() const;
    void createErrorNode(const Exception & exception, Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper);

    /// Deserializes an error stored in the error node.
    std::pair<std::exception_ptr, String> parseErrorNode(const String & error_node_contents) const;

    /// Reset the `connected` flag for each host.
    void resetConnectedFlag();

    /// Checks if the current query is cancelled, and if so then the function sets the `cancelled` flag in the current state.
    void checkIfQueryCancelled();

    /// Checks if the current state contains an error, and if so then the function passes this error to the query status
    /// to cancel the current BACKUP or RESTORE command.
    void cancelQueryIfError();

    /// Checks if some host was disconnected for too long, and if so then the function generates an error and pass it to the query status
    /// to cancel the current BACKUP or RESTORE command.
    void cancelQueryIfDisconnectedTooLong();

    /// Used by waitForHostsToReachStage() to check if everything is ready to return.
    bool checkIfHostsReachStage(const Strings & hosts, const String & stage_to_wait, Strings & results) const TSA_REQUIRES(mutex);

    /// Creates the 'finish' node.
    void finishImpl(bool throw_if_error, WithRetries::Kind retries_kind);
    void createFinishNodeAndRemoveAliveNode(Coordination::ZooKeeperWithFaultInjection::Ptr zookeeper, bool throw_if_error);

    /// Returns the version used by the initiator.
    int getInitiatorVersion() const;

    /// Waits until all the other hosts finish their work.
    void waitOtherHostsFinishImpl(const String & reason, std::optional<std::chrono::seconds> timeout, bool throw_if_error) const;
    bool checkIfOtherHostsFinish(const String & reason, std::optional<std::chrono::milliseconds> timeout, bool time_is_out, bool throw_if_error) const TSA_REQUIRES(mutex);

    /// Returns true if all the hosts have finished.
    bool allHostsFinishedNoLock() const TSA_REQUIRES(mutex);
    bool finishedNoLock() const TSA_REQUIRES(mutex);
    bool otherHostsFinishedNoLock() const TSA_REQUIRES(mutex);

    /// Returns a list of the hosts which haven't finished yet.
    Strings getUnfinishedHostsNoLock() const TSA_REQUIRES(mutex);
    Strings getUnfinishedOtherHostsNoLock() const TSA_REQUIRES(mutex);

    const bool is_restore;
    const String operation_name;
    const String current_host;
    const String current_host_desc;
    const Strings all_hosts;
    const bool allow_concurrency;
    BackupConcurrencyCounters & concurrency_counters;

    /// A reference to a field of the parent object which is either BackupCoordinationOnCluster or RestoreCoordinationOnCluster.
    const WithRetries & with_retries;

    const ThreadPoolCallbackRunnerUnsafe<void> schedule;
    const QueryStatusPtr process_list_element;
    const LoggerPtr log;

    const std::chrono::seconds failure_after_host_disconnected_for_seconds;
    const std::chrono::seconds finish_timeout_after_error;
    const std::chrono::milliseconds sync_period_ms;
    const size_t max_attempts_after_bad_version;

    /// Paths in ZooKeeper.
    const std::filesystem::path zookeeper_path;
    const String root_zookeeper_path;
    const String operation_zookeeper_path;
    const String operation_node_name;
    const String start_node_path;
    const String finish_node_path;
    const String num_hosts_node_path;
    const String error_node_path;
    const String alive_node_path;
    const String alive_tracker_node_path;

    std::optional<BackupConcurrencyCheck> local_concurrency_check;

    std::shared_ptr<Poco::Event> zk_nodes_changed;

    /// We store list of previously found ZooKeeper nodes to show better logging messages.
    Strings zk_nodes;

    /// Information about one host read from ZooKeeper.
    struct HostInfo
    {
        String host;
        bool started = false;
        bool connected = false;
        bool finished = false;
        int version = 1;
        std::map<String /* stage */, String /* result */> stages = {}; /// std::map because we need to compare states
        std::exception_ptr exception = nullptr;

        std::chrono::time_point<std::chrono::system_clock> last_connection_time = {};
        std::chrono::time_point<std::chrono::steady_clock> last_connection_time_monotonic = {};

        bool operator ==(const HostInfo & other) const;
        bool operator !=(const HostInfo & other) const;
    };

    /// Information about all the host participating in the current BACKUP or RESTORE operation.
    /// This information is read from ZooKeeper.
    /// To simplify the programming logic `state` can only be updated AFTER changing corresponding nodes in ZooKeeper
    /// (for example, first we create the 'error' node, and only after that we set or read from ZK the `state.host_with_error` field).
    struct State
    {
        std::map<String /* host */, HostInfo> hosts; /// std::map because we need to compare states
        std::optional<String> host_with_error;

        bool operator ==(const State & other) const;
        bool operator !=(const State & other) const;

        void merge(const State & other);
        void addErrorInfo(std::exception_ptr exception, const String & host);
    };

    State state TSA_GUARDED_BY(mutex);
    mutable std::condition_variable state_changed;

    std::future<void> watching_thread_future;
    bool should_stop_watching_thread TSA_GUARDED_BY(mutex) = false;

    bool query_is_sent_to_other_hosts TSA_GUARDED_BY(mutex) = false;
    bool tried_to_finish[2] TSA_GUARDED_BY(mutex) = {false, false};
    bool tried_to_set_error TSA_GUARDED_BY(mutex) = false;

    mutable std::mutex mutex;
};

}
