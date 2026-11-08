#include <Storages/Kafka/Clients/Protocol/Messages/ListOffsets.h>

namespace Kafka::Protocol
{

void ListOffsetsRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "ListOffsets");
    const bool flex = flexible(version);
    w.writeInt32(replica_id);
    w.writeInt8(isolation_level);
    w.writeArr(topics, flex, [flex](Writer & wr, const ListOffsetsTopic & topic)
    {
        wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partitions, flex, [flex](Writer & wp, const ListOffsetsPartition & partition)
        {
            wp.writeInt32(partition.partition_index);
            wp.writeInt32(partition.current_leader_epoch);
            wp.writeInt64(partition.timestamp);
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeTags(flex);
}

ListOffsetsRequest ListOffsetsRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "ListOffsets");
    const bool flex = flexible(version);
    ListOffsetsRequest req;
    req.replica_id = r.readInt32();
    req.isolation_level = r.readInt8();
    req.topics = r.readArr<ListOffsetsTopic>(flex, [flex](Reader & rd)
    {
        ListOffsetsTopic topic;
        topic.name = rd.readStr(flex);
        topic.partitions = rd.readArr<ListOffsetsPartition>(flex, [flex](Reader & rp)
        {
            ListOffsetsPartition partition;
            partition.partition_index = rp.readInt32();
            partition.current_leader_epoch = rp.readInt32();
            partition.timestamp = rp.readInt64();
            rp.readTags(flex);
            return partition;
        });
        rd.readTags(flex);
        return topic;
    });
    r.readTags(flex);
    return req;
}

void ListOffsetsResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, ListOffsetsRequest::min_version, ListOffsetsRequest::max_version, "ListOffsets");
    const bool flex = ListOffsetsRequest::flexible(version);
    w.writeInt32(throttle_time_ms);
    w.writeArr(topics, flex, [flex](Writer & wr, const ListOffsetsTopicResponse & topic)
    {
        wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partitions, flex, [flex](Writer & wp, const ListOffsetsPartitionResponse & partition)
        {
            wp.writeInt32(partition.partition_index);
            wp.writeInt16(partition.error_code);
            wp.writeInt64(partition.timestamp);
            wp.writeInt64(partition.offset);
            wp.writeInt32(partition.leader_epoch);
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeTags(flex);
}

ListOffsetsResponse ListOffsetsResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, ListOffsetsRequest::min_version, ListOffsetsRequest::max_version, "ListOffsets");
    const bool flex = ListOffsetsRequest::flexible(version);
    ListOffsetsResponse resp;
    resp.throttle_time_ms = r.readInt32();
    resp.topics = r.readArr<ListOffsetsTopicResponse>(flex, [flex](Reader & rd)
    {
        ListOffsetsTopicResponse topic;
        topic.name = rd.readStr(flex);
        topic.partitions = rd.readArr<ListOffsetsPartitionResponse>(flex, [flex](Reader & rp)
        {
            ListOffsetsPartitionResponse partition;
            partition.partition_index = rp.readInt32();
            partition.error_code = rp.readInt16();
            partition.timestamp = rp.readInt64();
            partition.offset = rp.readInt64();
            partition.leader_epoch = rp.readInt32();
            rp.readTags(flex);
            return partition;
        });
        rd.readTags(flex);
        return topic;
    });
    r.readTags(flex);
    return resp;
}

}
