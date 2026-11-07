#include <Storages/Kafka/Clients/Client.h>

#include <Storages/Kafka/Clients/Ops.h>

namespace Kafka
{

using Net::Deadline;

struct Client::Impl
{
    explicit Impl(ClientConfig config) : cluster(std::move(config)) { }
    Cluster cluster;
};

Client::Client(ClientConfig config) : impl(std::make_unique<Impl>(std::move(config)))
{
}

Client::~Client() = default;

Cluster & Client::getCluster()
{
    return impl->cluster;
}

void Client::wakeup()
{
    impl->cluster.wakeup();
}

ClusterMetadata Client::getMetadata(const std::vector<String> & topics, std::chrono::milliseconds timeout)
{
    return Ops::getMetadata(impl->cluster, topics, Deadline::after(timeout));
}

void Client::ensureTopic(const String & topic, std::chrono::milliseconds timeout)
{
    Ops::ensureTopic(impl->cluster, topic, Deadline::after(timeout));
}

FetchResult Client::fetch(const FetchSpec & spec, std::chrono::milliseconds timeout)
{
    return Ops::fetch(impl->cluster, spec, Deadline::after(timeout));
}

Int64 Client::listOffset(const TopicPartition & tp, OffsetSpec spec, std::chrono::milliseconds timeout)
{
    return Ops::listOffset(impl->cluster, tp, spec, Deadline::after(timeout));
}

void Client::commitOffsets(const String & group, const std::vector<TopicPartitionOffset> & offsets, std::chrono::milliseconds timeout)
{
    Ops::commitOffsets(impl->cluster, group, offsets, Deadline::after(timeout));
}

std::vector<TopicPartitionOffset>
Client::fetchCommittedOffsets(const String & group, const std::vector<TopicPartition> & partitions, std::chrono::milliseconds timeout)
{
    return Ops::fetchCommittedOffsets(impl->cluster, group, partitions, Deadline::after(timeout));
}

Client::ProduceResult Client::produce(const ProduceSpec & spec, std::chrono::milliseconds timeout)
{
    return Ops::produce(impl->cluster, spec, Deadline::after(timeout));
}

}
