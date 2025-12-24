#include <Storages/Kafka/SimpleTopicPartition.h>
#include <Common/SipHash.h>
#include <tuple>

namespace DB
{
std::size_t SimpleTopicPartitionHash::operator()(const SimpleTopicPartition & tp) const
{
    SipHash s;
    s.update(tp.topic);
    s.update(tp.partition_id);
    return s.get64();
}

bool SimpleTopicPartition::operator<(const SimpleTopicPartition & other) const
{
    return std::tie(topic, partition_id) < std::tie(other.topic, other.partition_id);
}


}
