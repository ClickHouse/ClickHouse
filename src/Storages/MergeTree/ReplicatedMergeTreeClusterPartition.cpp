#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteIntText.h>
#include <Core/NamesAndTypes.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

constexpr char HEADER[] = "cluster partition format version: 1\n";

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(const String & partition_id_)
    : partition_id(partition_id_)
{}

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(
    const String & partition_id_,
    const Strings & all_replicas_,
    const Strings & active_replicas_,
    const String & source_replica_,
    const String & new_replica_,
    const Coordination::Stat & stat)
    : partition_id(partition_id_)
    , all_replicas(all_replicas_)
    , active_replicas(active_replicas_)
    , source_replica(source_replica_)
    , new_replica(new_replica_)
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
    const auto & all_replicas = read_replicas("all_replicas");
    const auto & active_replicas = read_replicas("active_replicas");
    const auto & source_replica = read_replica("source_replica");
    const auto & new_replica = read_replica("new_replica");
    assertChar('\n', in);

    return ReplicatedMergeTreeClusterPartition(partition_id, all_replicas, active_replicas, source_replica, new_replica, stat);
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
    write_replicas("all_replicas", all_replicas);
    write_replicas("active_replicas", active_replicas);
    write_replica("source_replica", source_replica);
    write_replica("new_replica", new_replica);
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
    if (!isUnderReSharding())
        return fmt::format("{} (replicas: [{}], version: {})",
            partition_id,
            fmt::join(all_replicas, ", "),
            version);

    return fmt::format("{} (all_replicas: [{}], active_replicas: [{}], migration {} -> {}, version: {})",
        partition_id,
        fmt::join(all_replicas, ", "),
        fmt::join(active_replicas, ", "),
        source_replica,
        new_replica,
        version);
}

void ReplicatedMergeTreeClusterPartition::replaceReplica(const String & src, const String & dest)
{
    if (!source_replica.empty() || !new_replica.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} already under migration", toStringForLog());

    new_replica = dest;
    source_replica = src;

    std::erase(active_non_migration_replicas, source_replica);
    std::erase(all_non_migration_replicas, source_replica);

    all_replicas.push_back(dest);
}

void ReplicatedMergeTreeClusterPartition::finishReplicaMigration()
{
    if (source_replica.empty() || new_replica.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} is not under migration", toStringForLog());

    size_t n;
    n = std::erase(all_replicas, source_replica);
    if (!n)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No {} in all replicas ({})", source_replica, fmt::join(all_replicas, "\n"));
    n = std::erase(active_replicas, source_replica);
    if (!n)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No {} in active replicas ({})", source_replica, fmt::join(active_replicas, "\n"));

    active_replicas.push_back(new_replica);

    source_replica.clear();
    new_replica.clear();
}

void ReplicatedMergeTreeClusterPartition::revertReplicaMigration()
{
    size_t n = std::erase(all_replicas, new_replica);
    if (!n)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition {} is not under migration", toStringForLog());

    source_replica.clear();
    new_replica.clear();
}

}
