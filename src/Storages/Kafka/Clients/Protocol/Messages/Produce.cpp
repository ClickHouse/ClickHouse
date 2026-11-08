#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>

namespace Kafka::Protocol
{

void ProduceRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "Produce");
    const bool flex = flexible(version);
    w.writeNullableStr(transactional_id, flex);
    w.writeInt16(acks);
    w.writeInt32(timeout_ms);
    w.writeArr(topic_data, flex, [flex](Writer & wr, const ProduceTopicData & topic)
    {
        wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partition_data, flex, [flex](Writer & wp, const ProducePartitionData & partition)
        {
            wp.writeInt32(partition.index);
            wp.writeNullableBytesViewField(partition.records, flex);
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeTags(flex);
}

ProduceRequest ProduceRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "Produce");
    const bool flex = flexible(version);
    ProduceRequest req;
    req.transactional_id = r.readNullableStr(flex);
    req.acks = r.readInt16();
    req.timeout_ms = r.readInt32();
    req.topic_data = r.readArr<ProduceTopicData>(flex, [flex](Reader & rd)
    {
        ProduceTopicData topic;
        topic.name = rd.readStr(flex);
        topic.partition_data = rd.readArr<ProducePartitionData>(flex, [flex](Reader & rp)
        {
            ProducePartitionData partition;
            partition.index = rp.readInt32();
            partition.records = rp.readNullableBytesViewField(flex);
            rp.readTags(flex);
            return partition;
        });
        rd.readTags(flex);
        return topic;
    });
    r.readTags(flex);
    return req;
}

void ProduceResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, ProduceRequest::min_version, ProduceRequest::max_version, "Produce");
    const bool flex = ProduceRequest::flexible(version);
    w.writeArr(responses, flex, [flex, version](Writer & wr, const ProduceTopicResponse & topic)
    {
        wr.writeStr(topic.name, flex);
        wr.writeArr(topic.partition_responses, flex, [flex, version](Writer & wp, const ProducePartitionResponse & partition)
        {
            wp.writeInt32(partition.index);
            wp.writeInt16(partition.error_code);
            wp.writeInt64(partition.base_offset);
            wp.writeInt64(partition.log_append_time_ms);
            wp.writeInt64(partition.log_start_offset);
            if (version >= 8)
            {
                wp.writeArr(partition.record_errors, flex, [flex](Writer & we, const RecordError & error)
                {
                    we.writeInt32(error.batch_index);
                    we.writeNullableStr(error.batch_index_error_message, flex);
                    we.writeTags(flex);
                });
                wp.writeNullableStr(partition.error_message, flex);
            }
            wp.writeTags(flex);
        });
        wr.writeTags(flex);
    });
    w.writeInt32(throttle_time_ms);
    w.writeTags(flex);
}

ProduceResponse ProduceResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, ProduceRequest::min_version, ProduceRequest::max_version, "Produce");
    const bool flex = ProduceRequest::flexible(version);
    ProduceResponse resp;
    resp.responses = r.readArr<ProduceTopicResponse>(flex, [flex, version](Reader & rd)
    {
        ProduceTopicResponse topic;
        topic.name = rd.readStr(flex);
        topic.partition_responses = rd.readArr<ProducePartitionResponse>(flex, [flex, version](Reader & rp)
        {
            ProducePartitionResponse partition;
            partition.index = rp.readInt32();
            partition.error_code = rp.readInt16();
            partition.base_offset = rp.readInt64();
            partition.log_append_time_ms = rp.readInt64();
            partition.log_start_offset = rp.readInt64();
            if (version >= 8)
            {
                partition.record_errors = rp.readArr<RecordError>(flex, [flex](Reader & re)
                {
                    RecordError error;
                    error.batch_index = re.readInt32();
                    error.batch_index_error_message = re.readNullableStr(flex);
                    re.readTags(flex);
                    return error;
                });
                partition.error_message = rp.readNullableStr(flex);
            }
            /// Tagged fields may carry current_leader (v10+) — skipped.
            rp.readTags(flex);
            return partition;
        });
        rd.readTags(flex);
        return topic;
    });
    resp.throttle_time_ms = r.readInt32();
    r.readTags(flex);
    return resp;
}

}
