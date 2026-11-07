#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>

namespace Kafka::Protocol
{

void MetadataRequest::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, min_version, max_version, "Metadata");
    w.writeCompactNullableArray(topics, [version](Writer & wr, const MetadataRequestTopic & topic)
    {
        if (version >= 10)
        {
            wr.writeUuid(topic.topic_id);
            wr.writeCompactNullableString(topic.name);
        }
        else
        {
            /// Below v10 the name is non-nullable (and there is no topic id).
            wr.writeCompactString(topic.name.value_or(""));
        }
        wr.writeEmptyTaggedFields();
    });
    w.writeBool(allow_auto_topic_creation);
    if (version <= 10)
        w.writeBool(false); /// include_cluster_authorized_operations (8-10)
    w.writeBool(include_topic_authorized_operations);
    w.writeEmptyTaggedFields();
}

MetadataRequest MetadataRequest::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, min_version, max_version, "Metadata");
    MetadataRequest req;
    req.topics = r.readCompactNullableArray<MetadataRequestTopic>([version](Reader & rd)
    {
        MetadataRequestTopic topic;
        if (version >= 10)
        {
            topic.topic_id = rd.readUuid();
            topic.name = rd.readCompactNullableString();
        }
        else
        {
            topic.name = rd.readCompactString();
        }
        rd.skipTaggedFields();
        return topic;
    });
    req.allow_auto_topic_creation = r.readBool();
    if (version <= 10)
        r.readBool(); /// include_cluster_authorized_operations (8-10)
    req.include_topic_authorized_operations = r.readBool();
    r.skipTaggedFields();
    return req;
}

void MetadataResponse::encode(Writer & w, Int16 version) const
{
    checkVersionSupported(version, MetadataRequest::min_version, MetadataRequest::max_version, "Metadata");
    w.writeInt32(throttle_time_ms);
    w.writeCompactArray(brokers, [](Writer & wr, const MetadataBroker & broker)
    {
        wr.writeInt32(broker.node_id);
        wr.writeCompactString(broker.host);
        wr.writeInt32(broker.port);
        wr.writeCompactNullableString(broker.rack);
        wr.writeEmptyTaggedFields();
    });
    w.writeCompactNullableString(cluster_id);
    w.writeInt32(controller_id);
    w.writeCompactArray(topics, [version](Writer & wr, const MetadataTopic & topic)
    {
        wr.writeInt16(topic.error_code);
        if (version >= 12)
            wr.writeCompactNullableString(topic.name);
        else
            wr.writeCompactString(topic.name.value_or(""));
        if (version >= 10)
            wr.writeUuid(topic.topic_id);
        wr.writeBool(topic.is_internal);
        wr.writeCompactArray(topic.partitions, [](Writer & wp, const MetadataPartition & partition)
        {
            wp.writeInt16(partition.error_code);
            wp.writeInt32(partition.partition_index);
            wp.writeInt32(partition.leader_id);
            wp.writeInt32(partition.leader_epoch);
            auto writeNode = [](Writer & wn, Int32 node) { wn.writeInt32(node); };
            wp.writeCompactArray(partition.replica_nodes, writeNode);
            wp.writeCompactArray(partition.isr_nodes, writeNode);
            wp.writeCompactArray(partition.offline_replicas, writeNode);
            wp.writeEmptyTaggedFields();
        });
        wr.writeInt32(topic.topic_authorized_operations);
        wr.writeEmptyTaggedFields();
    });
    if (version <= 10)
        w.writeInt32(INT32_MIN); /// cluster_authorized_operations (8-10)
    w.writeEmptyTaggedFields();
}

MetadataResponse MetadataResponse::decode(Reader & r, Int16 version)
{
    checkVersionSupported(version, MetadataRequest::min_version, MetadataRequest::max_version, "Metadata");
    MetadataResponse resp;
    resp.throttle_time_ms = r.readInt32();
    resp.brokers = r.readCompactArray<MetadataBroker>([](Reader & rd)
    {
        MetadataBroker broker;
        broker.node_id = rd.readInt32();
        broker.host = rd.readCompactString();
        broker.port = rd.readInt32();
        broker.rack = rd.readCompactNullableString();
        rd.skipTaggedFields();
        return broker;
    });
    resp.cluster_id = r.readCompactNullableString();
    resp.controller_id = r.readInt32();
    resp.topics = r.readCompactArray<MetadataTopic>([version](Reader & rd)
    {
        MetadataTopic topic;
        topic.error_code = rd.readInt16();
        if (version >= 12)
            topic.name = rd.readCompactNullableString();
        else
            topic.name = rd.readCompactString();
        if (version >= 10)
            topic.topic_id = rd.readUuid();
        topic.is_internal = rd.readBool();
        topic.partitions = rd.readCompactArray<MetadataPartition>([](Reader & rp)
        {
            MetadataPartition partition;
            partition.error_code = rp.readInt16();
            partition.partition_index = rp.readInt32();
            partition.leader_id = rp.readInt32();
            partition.leader_epoch = rp.readInt32();
            auto readNode = [](Reader & rn) { return rn.readInt32(); };
            partition.replica_nodes = rp.readCompactArray<Int32>(readNode);
            partition.isr_nodes = rp.readCompactArray<Int32>(readNode);
            partition.offline_replicas = rp.readCompactArray<Int32>(readNode);
            rp.skipTaggedFields();
            return partition;
        });
        topic.topic_authorized_operations = rd.readInt32();
        rd.skipTaggedFields();
        return topic;
    });
    if (version <= 10)
        r.readInt32(); /// cluster_authorized_operations (8-10)
    r.skipTaggedFields();
    return resp;
}

}
