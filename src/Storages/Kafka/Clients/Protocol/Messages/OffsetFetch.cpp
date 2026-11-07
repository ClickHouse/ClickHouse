#include <Storages/Kafka/Clients/Protocol/Messages/OffsetFetch.h>

namespace Kafka::Protocol
{

namespace
{

void encodeTopics(Writer & w, const std::optional<std::vector<OffsetFetchTopic>> & topics)
{
    w.writeCompactNullableArray(topics, [](Writer & wt, const OffsetFetchTopic & topic)
    {
        wt.writeCompactString(topic.name);
        wt.writeCompactArray(topic.partition_indexes, [](Writer & wp, Int32 p) { wp.writeInt32(p); });
        wt.writeEmptyTaggedFields();
    });
}

std::optional<std::vector<OffsetFetchTopic>> decodeTopics(Reader & r)
{
    return r.readCompactNullableArray<OffsetFetchTopic>([](Reader & rt)
    {
        OffsetFetchTopic topic;
        topic.name = rt.readCompactString();
        topic.partition_indexes = rt.readCompactArray<Int32>([](Reader & rp) { return rp.readInt32(); });
        rt.skipTaggedFields();
        return topic;
    });
}

void encodePartitions(Writer & w, const std::vector<OffsetFetchPartitionResponse> & partitions)
{
    w.writeCompactArray(partitions, [](Writer & wp, const OffsetFetchPartitionResponse & partition)
    {
        wp.writeInt32(partition.partition_index);
        wp.writeInt64(partition.committed_offset);
        wp.writeInt32(partition.committed_leader_epoch);
        wp.writeCompactNullableString(partition.metadata);
        wp.writeInt16(partition.error_code);
        wp.writeEmptyTaggedFields();
    });
}

std::vector<OffsetFetchPartitionResponse> decodePartitions(Reader & r)
{
    return r.readCompactArray<OffsetFetchPartitionResponse>([](Reader & rp)
    {
        OffsetFetchPartitionResponse partition;
        partition.partition_index = rp.readInt32();
        partition.committed_offset = rp.readInt64();
        partition.committed_leader_epoch = rp.readInt32();
        partition.metadata = rp.readCompactNullableString();
        partition.error_code = rp.readInt16();
        rp.skipTaggedFields();
        return partition;
    });
}

}

void OffsetFetchRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "OffsetFetch");
    if (version >= 8)
    {
        w.writeCompactArray(groups, [](Writer & wr, const OffsetFetchGroup & group)
        {
            wr.writeCompactString(group.group_id);
            /// v9 would emit MemberId + MemberEpoch here.
            encodeTopics(wr, group.topics);
            wr.writeEmptyTaggedFields();
        });
        w.writeBool(require_stable);
    }
    else
    {
        /// v6-v7: one group at the top level.
        if (groups.size() != 1)
            throw Exception(ErrorKind::Protocol, "OffsetFetch v{} carries exactly one group, got {}", version, groups.size());
        w.writeCompactString(groups.front().group_id);
        encodeTopics(w, groups.front().topics);
        if (version >= 7)
            w.writeBool(require_stable);
    }
    w.writeEmptyTaggedFields();
}

OffsetFetchRequest OffsetFetchRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "OffsetFetch");
    OffsetFetchRequest req;
    if (version >= 8)
    {
        req.groups = r.readCompactArray<OffsetFetchGroup>([](Reader & rd)
        {
            OffsetFetchGroup group;
            group.group_id = rd.readCompactString();
            group.topics = decodeTopics(rd);
            rd.skipTaggedFields();
            return group;
        });
        req.require_stable = r.readBool();
    }
    else
    {
        OffsetFetchGroup group;
        group.group_id = r.readCompactString();
        group.topics = decodeTopics(r);
        req.groups = {std::move(group)};
        if (version >= 7)
            req.require_stable = r.readBool();
    }
    r.skipTaggedFields();
    return req;
}

void OffsetFetchResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, OffsetFetchRequest::min_version, OffsetFetchRequest::max_version, "OffsetFetch");
    w.writeInt32(throttle_time_ms);
    if (version >= 8)
    {
        w.writeCompactArray(groups, [](Writer & wr, const OffsetFetchGroupResponse & group)
        {
            wr.writeCompactString(group.group_id);
            wr.writeCompactArray(group.topics, [](Writer & wt, const OffsetFetchTopicResponse & topic)
            {
                wt.writeCompactString(topic.name);
                encodePartitions(wt, topic.partitions);
                wt.writeEmptyTaggedFields();
            });
            wr.writeInt16(group.error_code);
            wr.writeEmptyTaggedFields();
        });
    }
    else
    {
        if (groups.size() != 1)
            throw Exception(ErrorKind::Protocol, "OffsetFetch v{} carries exactly one group, got {}", version, groups.size());
        const auto & group = groups.front();
        w.writeCompactArray(group.topics, [](Writer & wt, const OffsetFetchTopicResponse & topic)
        {
            wt.writeCompactString(topic.name);
            encodePartitions(wt, topic.partitions);
            wt.writeEmptyTaggedFields();
        });
        w.writeInt16(group.error_code);
    }
    w.writeEmptyTaggedFields();
}

OffsetFetchResponse OffsetFetchResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, OffsetFetchRequest::min_version, OffsetFetchRequest::max_version, "OffsetFetch");
    OffsetFetchResponse resp;
    resp.throttle_time_ms = r.readInt32();
    if (version >= 8)
    {
        resp.groups = r.readCompactArray<OffsetFetchGroupResponse>([](Reader & rd)
        {
            OffsetFetchGroupResponse group;
            group.group_id = rd.readCompactString();
            group.topics = rd.readCompactArray<OffsetFetchTopicResponse>([](Reader & rt)
            {
                OffsetFetchTopicResponse topic;
                topic.name = rt.readCompactString();
                topic.partitions = decodePartitions(rt);
                rt.skipTaggedFields();
                return topic;
            });
            group.error_code = rd.readInt16();
            rd.skipTaggedFields();
            return group;
        });
    }
    else
    {
        /// v6-v7: flat single group; the group id is not echoed back.
        OffsetFetchGroupResponse group;
        group.topics = r.readCompactArray<OffsetFetchTopicResponse>([](Reader & rt)
        {
            OffsetFetchTopicResponse topic;
            topic.name = rt.readCompactString();
            topic.partitions = decodePartitions(rt);
            rt.skipTaggedFields();
            return topic;
        });
        group.error_code = r.readInt16();
        resp.groups = {std::move(group)};
    }
    r.skipTaggedFields();
    return resp;
}

}
