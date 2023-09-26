#include <optional>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteIntText.h>
#include <Core/NamesAndTypes.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <base/defines.h>
#include <magic_enum.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
};

constexpr char HEADER[] = "cluster partition format version: 1\n";

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(const String & partition_id_)
    : partition_id(partition_id_)
{}

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(const String & partition_id_, const Strings & replicas)
    : ReplicatedMergeTreeClusterPartition(partition_id_,
        UP_TO_DATE,
        /* all_replicas_= */ replicas,
        /* active_replicas_= */ replicas,
        /* source_replica_= */ String(),
        /* new_replica_= */ String(),
        /* drop_replica_= */ String(),
        /* stat= */ Coordination::Stat()
    )
{}

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(
    const String & partition_id_,
    ReplicatedMergeTreeClusterPartitionState state_,
    const Strings & all_replicas_,
    const Strings & active_replicas_,
    const String & source_replica_,
    const String & new_replica_,
    const String & drop_replica_,
    const Coordination::Stat & stat)
    : partition_id(partition_id_)
    , state(state_)
    , all_replicas(all_replicas_)
    , active_replicas(active_replicas_)
    , source_replica(source_replica_)
    , new_replica(new_replica_)
    , drop_replica(drop_replica_)
    , version(stat.version)
    , modification_time_ms(stat.mtime)
{
    active_non_migration_replicas = active_replicas;
    all_non_migration_replicas = all_replicas;
    if (!source_replica.empty())
    {
        std::erase(active_non_migration_replicas, source_replica);
        std::erase(all_non_migration_replicas, source_replica);
    }
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeClusterPartition::read(ReadBuffer & in, const Coordination::Stat & stat, const String & partition_id)
{
    auto read_replicas = [&in](const std::string_view & name)
    {
        size_t replicas_num;

        assertString(fmt::format("{} ", name), in);
        readIntText(replicas_num, in);
        assertString(":\n", in);

        Strings result_replicas(replicas_num);
        for (auto & replica : result_replicas)
        {
            readEscapedString(replica, in);
            assertChar('\n', in);
        }
        return result_replicas;
    };
    auto read_replica = [&in](const std::string_view & name)
    {
        String replica;
        assertString(fmt::format("{}: ", name), in);
        readEscapedString(replica, in);
        assertChar('\n', in);
        return replica;
    };

    assertString(HEADER, in);

    /// state
    std::optional<ReplicatedMergeTreeClusterPartitionState> state;
    {
        assertString("state: ", in);
        String state_string;
        readString(state_string, in);
        assertChar('\n', in);
        state = magic_enum::enum_cast<ReplicatedMergeTreeClusterPartitionState>(state_string);
        if (!state.has_value())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse state, got: {}", state_string);
    }

    const auto & all_replicas = read_replicas("all_replicas");
    const auto & active_replicas = read_replicas("active_replicas");
    const auto & source_replica = read_replica("source_replica");
    const auto & new_replica = read_replica("new_replica");
    const auto & drop_replica = read_replica("drop_replica");
    assertChar('\n', in);

    return ReplicatedMergeTreeClusterPartition(partition_id, state.value(),
        all_replicas, active_replicas,
        source_replica, new_replica, drop_replica,
        stat);
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeClusterPartition::fromString(const String & str, const Coordination::Stat & stat, const String & partition_id)
try
{
    ReadBufferFromString in(str);
    return read(in, stat, partition_id);
}
catch (Exception & e)
{
    e.addMessage("while parsing entry for partition {}:\n{}", partition_id, str);
    throw;
}

void ReplicatedMergeTreeClusterPartition::write(WriteBuffer & out) const
{
    auto write_replicas = [&out](const std::string_view name, const auto & replicas)
    {
        writeString(fmt::format("{} {}:\n", name, replicas.size()), out);
        for (const auto & replica_name : replicas)
        {
            writeEscapedString(replica_name, out);
            writeChar('\n', out);
        }
    };
    auto write_replica = [&out](const std::string_view name, const auto & replica)
    {
        writeString(fmt::format("{}: ", name), out);
        writeEscapedString(replica, out);
        writeChar('\n', out);
    };

    writeString(HEADER, out);
    writeString(fmt::format("state: {}\n", magic_enum::enum_name(state)), out);
    write_replicas("all_replicas", all_replicas);
    write_replicas("active_replicas", active_replicas);
    write_replica("source_replica", source_replica);
    write_replica("new_replica", new_replica);
    write_replica("drop_replica", drop_replica);
    writeChar('\n', out);
}

String ReplicatedMergeTreeClusterPartition::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

String ReplicatedMergeTreeClusterPartition::toStringForLog() const
{
    switch (state)
    {
        case UP_TO_DATE:
            return fmt::format("{} (replicas: [{}], version: {})",
                partition_id,
                fmt::join(all_replicas, ", "),
                version);
        case DROPPING:
            return fmt::format("{} (all_replicas: [{}], active_replicas: [{}], {} {}, version: {})",
                partition_id,
                fmt::join(all_replicas, ", "),
                fmt::join(active_replicas, ", "),
                magic_enum::enum_name(state),
                drop_replica,
                version);
        case MIGRATING:
        case CLONING:
            return fmt::format("{} (all_replicas: [{}], active_replicas: [{}], {} {} -> {}, version: {})",
                partition_id,
                fmt::join(all_replicas, ", "),
                fmt::join(active_replicas, ", "),
                magic_enum::enum_name(state),
                source_replica,
                new_replica,
                version);
    }
}

bool ReplicatedMergeTreeClusterPartition::hasReplica(const String & replica) const
{
    return std::find(all_replicas.begin(), all_replicas.end(), replica) != all_replicas.end();
}

void ReplicatedMergeTreeClusterPartition::removeReplica(const String & replica)
{
    /// NOTE: It is safe to do this because we have synchronization via version in coordinator.
    ///
    /// We have following interested persons:
    ///
    /// - balancer -- after it finishes the job, it will update the partition
    ///               with incrementing the version, so if this fails, it will
    ///               revert the operation, but in case of revert it should
    ///               also check is this replica still in list.
    ///
    /// - remover --  DROP/CREATE TABLE will update the partition version as
    ///               well, and retry in case of ZBADVERSION.
    auto old_state = state;
    if (state != UP_TO_DATE)
        revert();
    if (!std::erase(all_replicas, replica) && old_state == UP_TO_DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No replica {} in all replicas ({})", replica, fmt::join(all_replicas, ", "));
    std::erase(active_replicas, replica);
}

void ReplicatedMergeTreeClusterPartition::replaceReplica(const String & src, const String & dest)
{
    if (state != UP_TO_DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} already under migration", toStringForLog());
    if (std::find(all_replicas.begin(), all_replicas.end(), dest) != all_replicas.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} already exists on replica {}", toStringForLog(), dest);

    new_replica = dest;
    source_replica = src;

    std::erase(active_non_migration_replicas, source_replica);
    std::erase(all_non_migration_replicas, source_replica);

    all_replicas.push_back(dest);

    state = MIGRATING;
}

void ReplicatedMergeTreeClusterPartition::addReplica(const String & src, const String & dest)
{
    if (state != UP_TO_DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} already under migration", toStringForLog());

    new_replica = dest;
    source_replica = src;

    all_replicas.push_back(dest);
    state = CLONING;
}

void ReplicatedMergeTreeClusterPartition::dropReplica(const String & replica)
{
    if (state != UP_TO_DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} already under migration", toStringForLog());

    drop_replica = replica;

    std::erase(active_non_migration_replicas, replica);
    std::erase(all_non_migration_replicas, replica);

    state = DROPPING;
}

void ReplicatedMergeTreeClusterPartition::finish()
{
    if (state == UP_TO_DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} is not under migration", toStringForLog());

    switch (state)
    {
        case UP_TO_DATE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} is not under migration", toStringForLog());
        case MIGRATING:
        {
            size_t n;
            n = std::erase(all_replicas, source_replica);
            if (!n)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No replica {} in all replicas ({})", source_replica, fmt::join(all_replicas, ", "));
            n = std::erase(active_replicas, source_replica);
            if (!n)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No replica {} in active replicas ({})", source_replica, fmt::join(active_replicas, ", "));

            active_replicas.push_back(new_replica);
            break;
        }
        case CLONING:
            active_replicas.push_back(new_replica);
            break;
        case DROPPING:
        {
            size_t n;
            n = std::erase(all_replicas, drop_replica);
            if (!n)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No replica {} in all replicas ({})", drop_replica, fmt::join(all_replicas, ", "));
            n = std::erase(active_replicas, drop_replica);
            if (!n)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No replica {} in active replicas ({})", drop_replica, fmt::join(active_replicas, ", "));

            active_replicas.push_back(new_replica);
            break;
        }
    }

    source_replica.clear();
    new_replica.clear();
    drop_replica.clear();

    state = UP_TO_DATE;
}

void ReplicatedMergeTreeClusterPartition::revert()
{
    switch (state)
    {
        case UP_TO_DATE:
        case DROPPING:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot revert partition {}", toStringForLog());
        case MIGRATING:
        case CLONING:
        {
            size_t n = std::erase(all_replicas, new_replica);
            if (!n)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} is not under migration", toStringForLog());
            break;
        }
    }

    source_replica.clear();
    new_replica.clear();
    state = UP_TO_DATE;
}

}
