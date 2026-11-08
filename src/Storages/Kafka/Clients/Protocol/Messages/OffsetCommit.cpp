#include <Storages/Kafka/Clients/Protocol/Messages/OffsetCommit.h>

namespace Kafka::Protocol
{

void OffsetCommitRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "OffsetCommit");
    w.writeCompactString(group_id);
    w.writeInt32(generation_id_or_member_epoch);
    w.writeCompactString(member_id);
    w.writeCompactNullableString(group_instance_id);
    w.writeCompactArray(topics, [](Writer & wr, const OffsetCommitTopic & topic)
    {
        wr.writeCompactString(topic.name);
        wr.writeCompactArray(topic.partitions, [](Writer & wp, const OffsetCommitPartition & partition)
        {
            wp.writeInt32(partition.partition_index);
            wp.writeInt64(partition.committed_offset);
            wp.writeInt32(partition.committed_leader_epoch);
            wp.writeCompactNullableString(partition.committed_metadata);
            wp.writeEmptyTaggedFields();
        });
        wr.writeEmptyTaggedFields();
    });
    w.writeEmptyTaggedFields();
}

OffsetCommitRequest OffsetCommitRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "OffsetCommit");
    OffsetCommitRequest req;
    req.group_id = r.readCompactString();
    req.generation_id_or_member_epoch = r.readInt32();
    req.member_id = r.readCompactString();
    req.group_instance_id = r.readCompactNullableString();
    req.topics = r.readCompactArray<OffsetCommitTopic>([](Reader & rd)
    {
        OffsetCommitTopic topic;
        topic.name = rd.readCompactString();
        topic.partitions = rd.readCompactArray<OffsetCommitPartition>([](Reader & rp)
        {
            OffsetCommitPartition partition;
            partition.partition_index = rp.readInt32();
            partition.committed_offset = rp.readInt64();
            partition.committed_leader_epoch = rp.readInt32();
            partition.committed_metadata = rp.readCompactNullableString();
            rp.skipTaggedFields();
            return partition;
        });
        rd.skipTaggedFields();
        return topic;
    });
    r.skipTaggedFields();
    return req;
}

void OffsetCommitResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, OffsetCommitRequest::min_version, OffsetCommitRequest::max_version, "OffsetCommit");
    w.writeInt32(throttle_time_ms);
    w.writeCompactArray(topics, [](Writer & wr, const OffsetCommitTopicResponse & topic)
    {
        wr.writeCompactString(topic.name);
        wr.writeCompactArray(topic.partitions, [](Writer & wp, const OffsetCommitPartitionResponse & partition)
        {
            wp.writeInt32(partition.partition_index);
            wp.writeInt16(partition.error_code);
            wp.writeEmptyTaggedFields();
        });
        wr.writeEmptyTaggedFields();
    });
    w.writeEmptyTaggedFields();
}

OffsetCommitResponse OffsetCommitResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, OffsetCommitRequest::min_version, OffsetCommitRequest::max_version, "OffsetCommit");
    OffsetCommitResponse resp;
    resp.throttle_time_ms = r.readInt32();
    resp.topics = r.readCompactArray<OffsetCommitTopicResponse>([](Reader & rd)
    {
        OffsetCommitTopicResponse topic;
        topic.name = rd.readCompactString();
        topic.partitions = rd.readCompactArray<OffsetCommitPartitionResponse>([](Reader & rp)
        {
            OffsetCommitPartitionResponse partition;
            partition.partition_index = rp.readInt32();
            partition.error_code = rp.readInt16();
            rp.skipTaggedFields();
            return partition;
        });
        rd.skipTaggedFields();
        return topic;
    });
    r.skipTaggedFields();
    return resp;
}

}
