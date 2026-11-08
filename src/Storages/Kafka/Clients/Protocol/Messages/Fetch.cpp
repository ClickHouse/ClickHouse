#include <Storages/Kafka/Clients/Protocol/Messages/Fetch.h>

namespace Kafka::Protocol
{

void FetchRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "Fetch");
    const bool flex = flexible(version);
    const bool by_id = usesTopicIds(version);
    w.writeInt32(replica_id);
    w.writeInt32(max_wait_ms);
    w.writeInt32(min_bytes);
    w.writeInt32(max_bytes);
    w.writeInt8(isolation_level);
    w.writeInt32(session_id);
    w.writeInt32(session_epoch);
    w.writeArr(topics, flex, [flex, by_id, version](Writer & wr, const FetchTopic & topic)
    {
        if (by_id)
            wr.writeUuid(topic.topic_id);
        else
            wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partitions, flex, [flex, version](Writer & wp, const FetchPartition & partition)
        {
            wp.writeInt32(partition.partition);
            wp.writeInt32(partition.current_leader_epoch);
            wp.writeInt64(partition.fetch_offset);
            if (version >= 12)
                wp.writeInt32(partition.last_fetched_epoch);
            wp.writeInt64(partition.log_start_offset);
            wp.writeInt32(partition.partition_max_bytes);
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeArr(forgotten_topics_data, flex, [flex, by_id](Writer & wr, const ForgottenTopic & topic)
    {
        if (by_id)
            wr.writeUuid(topic.topic_id);
        else
            wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partitions, flex, [](Writer & wp, Int32 p) { wp.writeInt32(p); });
        wr.writeTags(flex);
    });
    w.writeStr(rack_id, flex);
    w.writeTags(flex);
}

FetchRequest FetchRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "Fetch");
    const bool flex = flexible(version);
    const bool by_id = usesTopicIds(version);
    FetchRequest req;
    req.replica_id = r.readInt32();
    req.max_wait_ms = r.readInt32();
    req.min_bytes = r.readInt32();
    req.max_bytes = r.readInt32();
    req.isolation_level = r.readInt8();
    req.session_id = r.readInt32();
    req.session_epoch = r.readInt32();
    req.topics = r.readArr<FetchTopic>(flex, [flex, by_id, version](Reader & rd)
    {
        FetchTopic topic;
        if (by_id)
            topic.topic_id = rd.readUuid();
        else
            topic.name = rd.readStr(flex);
        topic.partitions = rd.readArr<FetchPartition>(flex, [flex, version](Reader & rp)
        {
            FetchPartition partition;
            partition.partition = rp.readInt32();
            partition.current_leader_epoch = rp.readInt32();
            partition.fetch_offset = rp.readInt64();
            if (version >= 12)
                partition.last_fetched_epoch = rp.readInt32();
            partition.log_start_offset = rp.readInt64();
            partition.partition_max_bytes = rp.readInt32();
            rp.readTags(flex);
            return partition;
        });
        rd.readTags(flex);
        return topic;
    });
    req.forgotten_topics_data = r.readArr<ForgottenTopic>(flex, [flex, by_id](Reader & rd)
    {
        ForgottenTopic topic;
        if (by_id)
            topic.topic_id = rd.readUuid();
        else
            topic.name = rd.readStr(flex);
        topic.partitions = rd.readArr<Int32>(flex, [](Reader & rp) { return rp.readInt32(); });
        rd.readTags(flex);
        return topic;
    });
    req.rack_id = r.readStr(flex);
    r.readTags(flex);
    return req;
}

void FetchResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, FetchRequest::min_version, FetchRequest::max_version, "Fetch");
    const bool flex = FetchRequest::flexible(version);
    const bool by_id = FetchRequest::usesTopicIds(version);
    w.writeInt32(throttle_time_ms);
    w.writeInt16(error_code);
    w.writeInt32(session_id);
    w.writeArr(responses, flex, [flex, by_id](Writer & wr, const FetchTopicResponse & topic)
    {
        if (by_id)
            wr.writeUuid(topic.topic_id);
        else
            wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partitions, flex, [flex](Writer & wp, const FetchPartitionResponse & partition)
        {
            wp.writeInt32(partition.partition_index);
            wp.writeInt16(partition.error_code);
            wp.writeInt64(partition.high_watermark);
            wp.writeInt64(partition.last_stable_offset);
            wp.writeInt64(partition.log_start_offset);
            wp.writeNullableArr(partition.aborted_transactions, flex, [flex](Writer & wa, const AbortedTransaction & txn)
            {
                wa.writeInt64(txn.producer_id);
                wa.writeInt64(txn.first_offset);
                wa.writeTags(flex);
            });
            wp.writeInt32(partition.preferred_read_replica);
            wp.writeNullableBytesViewField(partition.records, flex);
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeTags(flex);
}

FetchResponse FetchResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, FetchRequest::min_version, FetchRequest::max_version, "Fetch");
    const bool flex = FetchRequest::flexible(version);
    const bool by_id = FetchRequest::usesTopicIds(version);
    FetchResponse resp;
    resp.throttle_time_ms = r.readInt32();
    resp.error_code = r.readInt16();
    resp.session_id = r.readInt32();
    resp.responses = r.readArr<FetchTopicResponse>(flex, [flex, by_id](Reader & rd)
    {
        FetchTopicResponse topic;
        if (by_id)
            topic.topic_id = rd.readUuid();
        else
            topic.name = rd.readStr(flex);
        topic.partitions = rd.readArr<FetchPartitionResponse>(flex, [flex](Reader & rp)
        {
            FetchPartitionResponse partition;
            partition.partition_index = rp.readInt32();
            partition.error_code = rp.readInt16();
            partition.high_watermark = rp.readInt64();
            partition.last_stable_offset = rp.readInt64();
            partition.log_start_offset = rp.readInt64();
            partition.aborted_transactions = rp.readNullableArr<AbortedTransaction>(flex, [flex](Reader & ra)
            {
                AbortedTransaction txn;
                txn.producer_id = ra.readInt64();
                txn.first_offset = ra.readInt64();
                ra.readTags(flex);
                return txn;
            });
            partition.preferred_read_replica = rp.readInt32();
            partition.records = rp.readNullableBytesViewField(flex);
            /// Tagged fields (v12+) may carry diverging_epoch / current_leader /
            /// snapshot_id — all skipped by the client.
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
