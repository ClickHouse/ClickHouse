#pragma once

/// Operation bodies over a Cluster. Split from the public Client shell so unit
/// tests can run them against a Cluster with an injected transport factory.

#include <Storages/Kafka/Clients/Client.h>
#include <Storages/Kafka/Clients/Cluster.h>

namespace Kafka::Ops
{

FetchResult fetch(Cluster & cluster, const FetchSpec & spec, const Net::Deadline & deadline);
Int64 listOffset(Cluster & cluster, const TopicPartition & tp, OffsetSpec spec, const Net::Deadline & deadline);
ClusterMetadata getMetadata(Cluster & cluster, const std::vector<String> & topics, const Net::Deadline & deadline);
void ensureTopic(Cluster & cluster, const String & topic, const Net::Deadline & deadline);
void commitOffsets(
    Cluster & cluster, const String & group, const std::vector<TopicPartitionOffset> & offsets, const Net::Deadline & deadline);
std::vector<TopicPartitionOffset> fetchCommittedOffsets(
    Cluster & cluster, const String & group, const std::vector<TopicPartition> & partitions, const Net::Deadline & deadline);
Client::ProduceResult produce(Cluster & cluster, const Client::ProduceSpec & spec, const Net::Deadline & deadline);

}
