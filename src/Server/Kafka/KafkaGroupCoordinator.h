#pragma once

#include <Server/Kafka/KafkaProtocol.h>
#include <Server/Kafka/KafkaTopics.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>


namespace DB::Kafka
{

/** The coordinator of consumer groups of the classic protocol (JoinGroup, SyncGroup, Heartbeat, LeaveGroup).
  * The state of the groups is kept in memory of this server, and every server is the coordinator of all groups,
  * so the members of a group must connect to the same server.
  * The committed offsets are stored in a table, see `commitOffsets`.
  */
class GroupCoordinator
{
public:
    static GroupCoordinator & instance();

    using IsCancelled = std::function<bool()>;

    struct JoinRequest
    {
        String group_id;
        Int32 session_timeout_ms = 0;
        Int32 rebalance_timeout_ms = 0;
        String member_id;
        std::optional<String> group_instance_id;
        String protocol_type;
        std::vector<std::pair<String, String>> protocols;
        String client_id;
        String client_host;
    };

    struct JoinedMember
    {
        String member_id;
        std::optional<String> group_instance_id;
        String metadata;
    };

    struct JoinResult
    {
        ErrorCode error = ErrorCode::NONE;
        Int32 generation_id = -1;
        String protocol_type;
        std::optional<String> protocol_name;
        String leader;
        String member_id;
        /// Only for the leader.
        std::vector<JoinedMember> members;
    };

    JoinResult joinGroup(const JoinRequest & request, UInt64 initial_rebalance_delay_ms, const IsCancelled & is_cancelled);

    struct SyncResult
    {
        ErrorCode error = ErrorCode::NONE;
        String protocol_type;
        String protocol_name;
        String assignment;
    };

    SyncResult syncGroup(
        const String & group_id,
        Int32 generation_id,
        const String & member_id,
        const std::vector<std::pair<String, String>> & assignments,
        const IsCancelled & is_cancelled);

    ErrorCode heartbeat(const String & group_id, Int32 generation_id, const String & member_id);
    ErrorCode leaveGroup(const String & group_id, const String & member_id);

    /// Whether a member may commit offsets for the group.
    ErrorCode validateOffsetCommit(const String & group_id, Int32 generation_id, const String & member_id);

    struct GroupListing
    {
        String group_id;
        String protocol_type;
        String state;
    };

    std::vector<GroupListing> listGroups();

    struct MemberDescription
    {
        String member_id;
        std::optional<String> group_instance_id;
        String client_id;
        String client_host;
        String metadata;
        String assignment;
    };

    struct GroupDescription
    {
        String group_id;
        String state;
        String protocol_type;
        String protocol_name;
        std::vector<MemberDescription> members;
    };

    GroupDescription describeGroup(const String & group_id);

    Int64 allocateProducerId() { return next_producer_id.fetch_add(1); }

private:
    GroupCoordinator();

    using Clock = std::chrono::steady_clock;

    struct Member
    {
        String member_id;
        std::optional<String> group_instance_id;
        String client_id;
        String client_host;
        Int32 session_timeout_ms = 0;
        Int32 rebalance_timeout_ms = 0;
        std::vector<std::pair<String, String>> protocols;
        String assignment;
        Clock::time_point last_heartbeat;
        /// Has joined in the current rebalance.
        bool joined = false;
        /// Is waiting in JoinGroup or SyncGroup, so it is alive.
        enum class Awaiting : uint8_t
        {
            None,
            Join,
            Sync,
        };
        Awaiting awaiting = Awaiting::None;
    };

    enum class State : uint8_t
    {
        Empty,
        PreparingRebalance,
        CompletingRebalance,
        Stable,
    };

    struct Group
    {
        State state = State::Empty;
        Int32 generation = 0;
        String protocol_type;
        String protocol_name;
        String leader;
        std::map<String, Member> members;
        Clock::time_point rebalance_deadline;
        Clock::time_point initial_delay_deadline;
    };

    static const char * stateName(State state);

    void prepareRebalance(Group & group, Clock::time_point now, std::chrono::milliseconds initial_delay);
    void tryCompleteJoin(Group & group, Clock::time_point now);
    void expireMembers(Group & group, Clock::time_point now);
    static String metadataForProtocol(const Member & member, const String & protocol);

    std::mutex mutex;
    std::condition_variable cv;
    std::map<String, Group> groups;

    std::atomic<Int64> next_producer_id;
};

struct CommittedOffset
{
    String topic;
    Int32 partition = 0;
    Int64 offset = -1;
    String metadata;
};

/// The committed offsets are stored in the table `BrokerSettings::offsets_table` (in the default database of topics),
/// created on the first commit as a `ReplacingMergeTree`. It can be created in advance with another engine,
/// e.g. `ReplicatedReplacingMergeTree`, with the columns `group_id`, `topic`, `partition`, `committed_offset`, `metadata`, `commit_time`.
void commitOffsets(const String & group_id, const std::vector<CommittedOffset> & offsets, const BrokerSettings & settings, const QueryContextFactory & make_context);

std::vector<CommittedOffset> fetchCommittedOffsets(const String & group_id, const BrokerSettings & settings, const QueryContextFactory & make_context);

}
