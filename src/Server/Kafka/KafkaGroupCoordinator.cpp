#include <Server/Kafka/KafkaGroupCoordinator.h>

#include <Columns/IColumn.h>
#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Core/UUID.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/WriteHelpers.h>

#include <algorithm>


namespace DB::Kafka
{

namespace
{

/// The waits are split into slices to notice the shutdown of the server.
constexpr auto max_wait_slice = std::chrono::milliseconds(1000);

}

GroupCoordinator & GroupCoordinator::instance()
{
    static GroupCoordinator coordinator;
    return coordinator;
}

GroupCoordinator::GroupCoordinator()
    /// The producer ids are unique across restarts of the server, because the batches are deduplicated by them.
    : next_producer_id(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count())
{
}

const char * GroupCoordinator::stateName(State state)
{
    switch (state)
    {
        case State::Empty: return "Empty";
        case State::PreparingRebalance: return "PreparingRebalance";
        case State::CompletingRebalance: return "CompletingRebalance";
        case State::Stable: return "Stable";
    }
}

String GroupCoordinator::metadataForProtocol(const Member & member, const String & protocol)
{
    for (const auto & [name, metadata] : member.protocols)
        if (name == protocol)
            return metadata;
    return {};
}

void GroupCoordinator::prepareRebalance(Group & group, Clock::time_point now, std::chrono::milliseconds initial_delay)
{
    const bool initial = group.state == State::Empty;
    group.state = State::PreparingRebalance;

    Int32 rebalance_timeout_ms = 0;
    for (auto & [_, member] : group.members)
    {
        member.joined = member.awaiting == Member::Awaiting::Join;
        rebalance_timeout_ms = std::max(rebalance_timeout_ms, member.rebalance_timeout_ms);
    }

    group.rebalance_deadline = now + std::chrono::milliseconds(rebalance_timeout_ms);
    group.initial_delay_deadline = initial ? now + initial_delay : now;
    group.rebalance_deadline = std::max(group.rebalance_deadline, group.initial_delay_deadline);
    cv.notify_all();
}

void GroupCoordinator::tryCompleteJoin(Group & group, Clock::time_point now)
{
    if (group.state != State::PreparingRebalance || now < group.initial_delay_deadline)
        return;

    bool all_joined = std::all_of(group.members.begin(), group.members.end(), [](const auto & member) { return member.second.joined; });
    if (!all_joined && now < group.rebalance_deadline)
        return;

    /// The members that did not rejoin in time are removed.
    std::erase_if(group.members, [](const auto & member) { return !member.second.joined; });

    ++group.generation;
    if (group.members.empty())
    {
        group.state = State::Empty;
        group.leader.clear();
        group.protocol_name.clear();
        cv.notify_all();
        return;
    }

    /// The first protocol of the leader supported by all members.
    if (!group.members.contains(group.leader))
        group.leader = group.members.begin()->first;

    group.protocol_name.clear();
    for (const auto & [name, _] : group.members.at(group.leader).protocols)
    {
        bool supported = std::all_of(group.members.begin(), group.members.end(), [&](const auto & member)
        {
            const auto & protocols = member.second.protocols;
            return std::any_of(protocols.begin(), protocols.end(), [&](const auto & protocol) { return protocol.first == name; });
        });
        if (supported)
        {
            group.protocol_name = name;
            break;
        }
    }

    for (auto & [_, member] : group.members)
        member.assignment.clear();

    group.state = State::CompletingRebalance;
    cv.notify_all();
}

void GroupCoordinator::expireMembers(Group & group, Clock::time_point now)
{
    size_t removed = std::erase_if(group.members, [&](const auto & member)
    {
        return member.second.awaiting == Member::Awaiting::None && now - member.second.last_heartbeat > std::chrono::milliseconds(member.second.session_timeout_ms);
    });

    if (!removed)
        return;

    if (group.members.empty() && group.state != State::PreparingRebalance)
    {
        group.state = State::Empty;
        ++group.generation;
        cv.notify_all();
    }
    else if (group.state == State::Stable || group.state == State::CompletingRebalance)
    {
        prepareRebalance(group, now, std::chrono::milliseconds(0));
    }
}

GroupCoordinator::JoinResult GroupCoordinator::joinGroup(const JoinRequest & request, UInt64 initial_rebalance_delay_ms, const IsCancelled & is_cancelled)
{
    JoinResult result;

    if (request.group_id.empty())
    {
        result.error = ErrorCode::INVALID_GROUP_ID;
        return result;
    }

    if (request.session_timeout_ms <= 0 || request.session_timeout_ms > 3600 * 1000 || request.protocols.empty())
    {
        result.error = ErrorCode::INVALID_SESSION_TIMEOUT;
        return result;
    }

    std::unique_lock lock(mutex);
    auto now = Clock::now();
    Group & group = groups[request.group_id];
    expireMembers(group, now);

    if (!group.members.empty())
    {
        if (group.protocol_type != request.protocol_type)
        {
            result.error = ErrorCode::INCONSISTENT_GROUP_PROTOCOL;
            return result;
        }

        /// At least one protocol must be supported by all members.
        bool has_common_protocol = false;
        for (const auto & [name, _] : request.protocols)
        {
            bool supported = std::all_of(group.members.begin(), group.members.end(), [&](const auto & member)
            {
                if (member.first == request.member_id)
                    return true;
                const auto & protocols = member.second.protocols;
                return std::any_of(protocols.begin(), protocols.end(), [&](const auto & protocol) { return protocol.first == name; });
            });
            if (supported)
            {
                has_common_protocol = true;
                break;
            }
        }
        if (!has_common_protocol)
        {
            result.error = ErrorCode::INCONSISTENT_GROUP_PROTOCOL;
            return result;
        }
    }

    String member_id = request.member_id;
    if (member_id.empty())
    {
        member_id = fmt::format("{}-{}", request.client_id.empty() ? "member" : request.client_id, toString(UUIDHelpers::generateV4()));
        group.members[member_id].member_id = member_id;
    }
    else if (!group.members.contains(member_id))
    {
        result.error = ErrorCode::UNKNOWN_MEMBER_ID;
        return result;
    }

    group.protocol_type = request.protocol_type;

    {
        Member & member = group.members.at(member_id);
        member.group_instance_id = request.group_instance_id;
        member.client_id = request.client_id;
        member.client_host = request.client_host;
        member.session_timeout_ms = request.session_timeout_ms;
        member.rebalance_timeout_ms = request.rebalance_timeout_ms > 0 ? request.rebalance_timeout_ms : request.session_timeout_ms;
        member.protocols = request.protocols;
        member.last_heartbeat = now;
        member.awaiting = Member::Awaiting::Join;
    }

    if (group.state != State::PreparingRebalance)
        prepareRebalance(group, now, std::chrono::milliseconds(initial_rebalance_delay_ms));

    group.members.at(member_id).joined = true;
    const Int32 target_generation = group.generation + 1;

    while (true)
    {
        now = Clock::now();
        expireMembers(group, now);
        tryCompleteJoin(group, now);

        auto it = group.members.find(member_id);
        if (it == group.members.end())
        {
            result.error = ErrorCode::UNKNOWN_MEMBER_ID;
            return result;
        }

        if (group.generation >= target_generation && group.state != State::PreparingRebalance)
            break;

        if (is_cancelled())
        {
            it->second.awaiting = Member::Awaiting::None;
            result.error = ErrorCode::COORDINATOR_NOT_AVAILABLE;
            return result;
        }

        auto deadline = std::min({now + max_wait_slice, std::max(group.initial_delay_deadline, now), group.rebalance_deadline});
        if (deadline <= now)
            deadline = now + max_wait_slice;
        cv.wait_until(lock, deadline);
    }

    Member & member = group.members.at(member_id);
    member.awaiting = Member::Awaiting::None;
    member.last_heartbeat = Clock::now();

    result.generation_id = group.generation;
    result.protocol_type = group.protocol_type;
    result.protocol_name = group.protocol_name;
    result.leader = group.leader;
    result.member_id = member_id;

    if (member_id == group.leader)
        for (const auto & [id, other] : group.members)
            result.members.push_back({id, other.group_instance_id, metadataForProtocol(other, group.protocol_name)});

    return result;
}

GroupCoordinator::SyncResult GroupCoordinator::syncGroup(
    const String & group_id,
    Int32 generation_id,
    const String & member_id,
    const std::vector<std::pair<String, String>> & assignments,
    const IsCancelled & is_cancelled)
{
    SyncResult result;

    std::unique_lock lock(mutex);
    auto group_it = groups.find(group_id);
    if (group_it == groups.end())
    {
        result.error = ErrorCode::UNKNOWN_MEMBER_ID;
        return result;
    }

    Group & group = group_it->second;
    auto now = Clock::now();
    expireMembers(group, now);

    auto it = group.members.find(member_id);
    if (it == group.members.end())
    {
        result.error = ErrorCode::UNKNOWN_MEMBER_ID;
        return result;
    }

    if (generation_id != group.generation)
    {
        result.error = ErrorCode::ILLEGAL_GENERATION;
        return result;
    }

    it->second.last_heartbeat = now;
    result.protocol_type = group.protocol_type;
    result.protocol_name = group.protocol_name;

    if (group.state == State::PreparingRebalance || group.state == State::Empty)
    {
        result.error = ErrorCode::REBALANCE_IN_PROGRESS;
        return result;
    }

    if (group.state == State::CompletingRebalance && member_id == group.leader)
    {
        for (const auto & [assigned_member, assignment] : assignments)
            if (auto assigned_it = group.members.find(assigned_member); assigned_it != group.members.end())
                assigned_it->second.assignment = assignment;

        group.state = State::Stable;
        cv.notify_all();
    }

    const auto deadline = now + std::chrono::milliseconds(it->second.session_timeout_ms);
    it->second.awaiting = Member::Awaiting::Sync;

    while (group.state == State::CompletingRebalance && group.generation == generation_id)
    {
        now = Clock::now();
        if (now >= deadline || is_cancelled())
            break;

        cv.wait_until(lock, std::min(deadline, now + max_wait_slice));
        expireMembers(group, Clock::now());
        if (!group.members.contains(member_id))
        {
            result.error = ErrorCode::UNKNOWN_MEMBER_ID;
            return result;
        }
    }

    Member & member = group.members.at(member_id);
    member.awaiting = Member::Awaiting::None;
    member.last_heartbeat = Clock::now();

    if (group.state != State::Stable || group.generation != generation_id)
    {
        result.error = ErrorCode::REBALANCE_IN_PROGRESS;
        return result;
    }

    result.assignment = member.assignment;
    return result;
}

ErrorCode GroupCoordinator::heartbeat(const String & group_id, Int32 generation_id, const String & member_id)
{
    std::lock_guard lock(mutex);
    auto group_it = groups.find(group_id);
    if (group_it == groups.end())
        return ErrorCode::UNKNOWN_MEMBER_ID;

    Group & group = group_it->second;
    auto now = Clock::now();
    expireMembers(group, now);

    auto it = group.members.find(member_id);
    if (it == group.members.end())
        return ErrorCode::UNKNOWN_MEMBER_ID;

    it->second.last_heartbeat = now;
    if (generation_id != group.generation)
        return ErrorCode::ILLEGAL_GENERATION;
    if (group.state == State::PreparingRebalance)
        return ErrorCode::REBALANCE_IN_PROGRESS;
    return ErrorCode::NONE;
}

ErrorCode GroupCoordinator::leaveGroup(const String & group_id, const String & member_id)
{
    std::lock_guard lock(mutex);
    auto group_it = groups.find(group_id);
    if (group_it == groups.end())
        return ErrorCode::UNKNOWN_MEMBER_ID;

    Group & group = group_it->second;
    if (!group.members.erase(member_id))
        return ErrorCode::UNKNOWN_MEMBER_ID;

    auto now = Clock::now();
    if (group.members.empty())
    {
        if (group.state != State::PreparingRebalance)
        {
            group.state = State::Empty;
            ++group.generation;
        }
        cv.notify_all();
    }
    else if (group.state != State::PreparingRebalance)
    {
        prepareRebalance(group, now, std::chrono::milliseconds(0));
    }
    else
    {
        tryCompleteJoin(group, now);
    }

    return ErrorCode::NONE;
}

ErrorCode GroupCoordinator::validateOffsetCommit(const String & group_id, Int32 generation_id, const String & member_id)
{
    if (group_id.empty())
        return ErrorCode::INVALID_GROUP_ID;

    std::lock_guard lock(mutex);
    auto group_it = groups.find(group_id);

    /// A commit outside of group membership, e.g. by a consumer with manually assigned partitions.
    if (generation_id < 0 && member_id.empty())
    {
        if (group_it == groups.end() || group_it->second.members.empty())
            return ErrorCode::NONE;
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }

    if (group_it == groups.end())
        return ErrorCode::UNKNOWN_MEMBER_ID;

    Group & group = group_it->second;
    expireMembers(group, Clock::now());

    auto it = group.members.find(member_id);
    if (it == group.members.end())
        return ErrorCode::UNKNOWN_MEMBER_ID;
    if (generation_id != group.generation)
        return ErrorCode::ILLEGAL_GENERATION;
    /// The members commit their offsets before rejoining, while the group is preparing a rebalance,
    /// but the assignment of the new generation is not known yet while it is completing.
    if (group.state == State::CompletingRebalance)
        return ErrorCode::REBALANCE_IN_PROGRESS;

    it->second.last_heartbeat = Clock::now();
    return ErrorCode::NONE;
}

std::vector<GroupCoordinator::GroupListing> GroupCoordinator::listGroups()
{
    std::lock_guard lock(mutex);
    std::vector<GroupListing> res;
    auto now = Clock::now();
    for (auto & [group_id, group] : groups)
    {
        expireMembers(group, now);
        if (group.members.empty() && group.state == State::Empty)
            continue;
        res.push_back({group_id, group.protocol_type, stateName(group.state)});
    }
    return res;
}

GroupCoordinator::GroupDescription GroupCoordinator::describeGroup(const String & group_id)
{
    std::lock_guard lock(mutex);
    GroupDescription res;
    res.group_id = group_id;

    auto it = groups.find(group_id);
    if (it == groups.end())
    {
        res.state = "Dead";
        return res;
    }

    Group & group = it->second;
    expireMembers(group, Clock::now());
    res.state = stateName(group.state);
    res.protocol_type = group.protocol_type;
    if (group.state == State::Stable || group.state == State::CompletingRebalance)
        res.protocol_name = group.protocol_name;

    for (const auto & [member_id, member] : group.members)
        res.members.push_back({member_id, member.group_instance_id, member.client_id, member.client_host,
            metadataForProtocol(member, group.protocol_name), member.assignment});

    return res;
}


namespace
{

String offsetsTableName(const BrokerSettings & settings)
{
    return backQuoteIfNeed(settings.database) + "." + backQuoteIfNeed(settings.offsets_table);
}

bool offsetsTableExists(const BrokerSettings & settings, ContextPtr context)
{
    return DatabaseCatalog::instance().isTableExist(StorageID(settings.database, settings.offsets_table), context);
}

}

void commitOffsets(const String & group_id, const std::vector<CommittedOffset> & offsets, const BrokerSettings & settings, const QueryContextFactory & make_context)
{
    if (offsets.empty())
        return;

    auto context = make_context();
    if (!offsetsTableExists(settings, context))
        runQuery(fmt::format(
            "CREATE TABLE IF NOT EXISTS {} (group_id String, topic String, partition Int32, committed_offset Int64, metadata String, commit_time DateTime64(6)) "
            "ENGINE = ReplacingMergeTree(commit_time) ORDER BY (group_id, topic, partition)",
            offsetsTableName(settings)), context);

    String query = fmt::format("INSERT INTO {} (group_id, topic, partition, committed_offset, metadata, commit_time) VALUES ", offsetsTableName(settings));
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        const auto & offset = offsets[i];
        query += fmt::format("{}({}, {}, {}, {}, {}, now64(6))",
            i ? ", " : "", quoteString(group_id), quoteString(offset.topic), offset.partition, offset.offset, quoteString(offset.metadata));
    }

    runQueryWithData(query, make_context());
}

std::vector<CommittedOffset> fetchCommittedOffsets(const String & group_id, const BrokerSettings & settings, const QueryContextFactory & make_context)
{
    std::vector<CommittedOffset> res;
    auto context = make_context();
    if (!offsetsTableExists(settings, context))
        return res;

    runSelect(
        fmt::format(
            "SELECT topic, partition, argMax(committed_offset, commit_time), argMax(metadata, commit_time) FROM {} WHERE group_id = {} GROUP BY topic, partition",
            offsetsTableName(settings), quoteString(group_id)),
        context,
        [&](const Block & block)
        {
            for (size_t row = 0; row < block.rows(); ++row)
                res.push_back({
                    String(block.getByPosition(0).column->getDataAt(row)),
                    static_cast<Int32>(block.getByPosition(1).column->getInt(row)),
                    block.getByPosition(2).column->getInt(row),
                    String(block.getByPosition(3).column->getDataAt(row))});
            return true;
        });

    return res;
}

}
