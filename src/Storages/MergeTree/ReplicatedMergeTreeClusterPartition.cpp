#include <Storages/MergeTree/ReplicatedMergeTreeClusterPartition.h>
#include <Core/NamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteIntText.h>

namespace
{

using namespace DB;

Strings makeReplicasNames(const ReplicatedMergeTreeClusterReplicas & replicas)
{
    Strings replicas_names;
    replicas_names.reserve(replicas.size());
    for (const auto & replica : replicas)
        replicas_names.emplace_back(replica.name);
    return replicas_names;
}

}

namespace DB
{

constexpr char HEADER[] = "cluster partition format version: 1\n";

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(const String & partition_id_)
    : partition_id(partition_id_)
{}

ReplicatedMergeTreeClusterPartition::ReplicatedMergeTreeClusterPartition(const String & partition_id_, const ReplicatedMergeTreeClusterReplicas & replicas_, int version_)
    : partition_id(partition_id_)
    , replicas(replicas_)
    , replicas_names(makeReplicasNames(replicas))
    , version(version_)
{
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeClusterPartition::read(ReadBuffer & in, int version, const String & partition_id, const ResolveReplica & resolve_replica)
{
    assertString(HEADER, in);

    ReplicatedMergeTreeClusterReplicas replicas;
    {
        assertString("replicas:\n", in);

        size_t replicas_num;
        readIntText(replicas_num, in);
        assertChar('\n', in);

        replicas.resize(replicas_num);
        for (auto & replica : replicas)
        {
            readEscapedString(replica.name, in);
            resolve_replica(replica);
            assertChar('\n', in);
        }
        assertChar('\n', in);
    }

    return ReplicatedMergeTreeClusterPartition(partition_id, replicas, version);
}

ReplicatedMergeTreeClusterPartition ReplicatedMergeTreeClusterPartition::fromString(const String & str, int version, const String & partition_id, const ResolveReplica & resolve_replica)
try
{
    ReadBufferFromString in(str);
    return read(in, version, partition_id, resolve_replica);
}
catch (Exception & e)
{
    e.addMessage("while parsing entry for partition {}:\n{}", partition_id, str);
    throw;
}

void ReplicatedMergeTreeClusterPartition::write(WriteBuffer & out) const
{
    writeString(HEADER, out);

    {
        writeString("replicas:\n", out);
        /// FIXME(cluster): remove number of replicas from format
        writeIntText(replicas.size(), out);
        writeChar('\n', out);
        for (const auto & replica : replicas)
        {
            writeEscapedString(replica.name, out);
            writeChar('\n', out);
        }
        writeChar('\n', out);
    }
}

String ReplicatedMergeTreeClusterPartition::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

String ReplicatedMergeTreeClusterPartition::toStringForLog() const
{
    return fmt::format("partition: {} (replicas: [{}], version: {})", partition_id, fmt::join(replicas_names, ", "), version);
}

}
