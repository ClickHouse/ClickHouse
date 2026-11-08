#include <Storages/Kafka/Clients/Ops.h>

#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Fetch.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ListOffsets.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetCommit.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetFetch.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>
#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <map>

namespace Kafka::Ops
{

using Net::Deadline;

namespace
{

constexpr int max_attempts = 3;
constexpr std::chrono::milliseconds retry_backoff{100};

/// Retry loop shared by the partition-leader operations: on stale-metadata
/// broker errors invalidate + retry, on connection errors retry on a fresh
/// connection, everything else surfaces.
template <typename Op>
auto withLeaderRetries(Cluster & cluster, const TopicPartition & tp, const Deadline & deadline, Op && op)
{
    for (int attempt = 1;; ++attempt)
    {
        try
        {
            const LeaderRef leader = cluster.leaderFor(tp, deadline);
            return op(leader);
        }
        catch (const Exception & e)
        {
            const bool metadata_stale = e.getKind() == ErrorKind::Broker && BrokerError::isMetadataStale(e.getBrokerCode());
            const bool connection_lost = e.getKind() == ErrorKind::Connection;
            if ((!metadata_stale && !connection_lost) || attempt >= max_attempts || deadline.expired())
                throw;
            LOG_DEBUG(
                cluster.getLogger(), "{}[{}]: attempt {} failed ({}), retrying", tp.topic, tp.partition, attempt, e.message());
            cluster.invalidateTopic(tp.topic);
            cluster.backoff(retry_backoff, deadline);
        }
    }
}

}

FetchResult fetch(Cluster & cluster, const FetchSpec & spec, const Deadline & deadline)
{
    const TopicPartition tp{spec.topic, spec.partition};

    return withLeaderRetries(cluster, tp, deadline, [&](const LeaderRef & leader)
    {
        auto lease = cluster.acquire(leader.node_id, leader.address, deadline);
        /// v13+ routes by topic UUID, v11-v12 by name. Decide once per call so
        /// the response matcher agrees with the request shape.
        const Int16 fetch_version = lease->negotiateVersion(
            Protocol::ApiKey::Fetch, Protocol::FetchRequest::min_version, Protocol::FetchRequest::max_version);
        const bool by_id = Protocol::FetchRequest::usesTopicIds(fetch_version);
        if (by_id && leader.topic_id.isZero())
            throw Exception(ErrorKind::Protocol, "Broker speaks Fetch v13+ but metadata carried no topic id for '{}'", spec.topic);

        const bool read_committed = cluster.getConfig().isolation_level == IsolationLevel::ReadCommitted;

        Protocol::FetchRequest request;
        /// The broker holds the request for up to max_wait_ms; keep a margin
        /// inside the caller deadline so the response still arrives in time.
        const auto max_wait_budget = deadline.remainingClamped(std::chrono::milliseconds{spec.max_wait_ms});
        request.max_wait_ms = static_cast<Int32>(std::max<Int64>(0, max_wait_budget.count() - 50));
        request.min_bytes = spec.min_bytes;
        request.max_bytes = std::max(spec.max_bytes, 1);
        request.isolation_level = read_committed ? 1 : 0;
        Protocol::FetchPartition partition;
        partition.partition = spec.partition;
        partition.current_leader_epoch = leader.leader_epoch;
        partition.fetch_offset = spec.offset;
        partition.partition_max_bytes = spec.max_bytes;
        Protocol::FetchTopic topic_entry;
        topic_entry.topic_id = leader.topic_id;
        topic_entry.name = spec.topic;
        topic_entry.partitions = {partition};
        request.topics = {std::move(topic_entry)};

        const auto response = lease->call(request, deadline);
        if (response.error_code != BrokerError::NONE)
            throw Exception(BrokerCode{response.error_code}, "Fetch failed");

        const Protocol::FetchPartitionResponse * p = nullptr;
        for (const auto & topic : response.responses)
            if (by_id ? topic.topic_id == leader.topic_id : topic.name == spec.topic)
                for (const auto & part : topic.partitions)
                    if (part.partition_index == spec.partition)
                        p = &part;
        if (!p)
            throw Exception(ErrorKind::Protocol, "Fetch response misses the requested partition");
        if (p->error_code != BrokerError::NONE)
            throw Exception(BrokerCode{p->error_code}, "Fetch {}[{}] failed", spec.topic, spec.partition);

        FetchResult result;
        result.high_watermark = p->high_watermark;
        result.log_start_offset = p->log_start_offset;
        result.last_stable_offset = p->last_stable_offset;
        result.next_offset = spec.offset;
        if (p->records && !p->records->empty())
        {
            std::vector<Protocol::AbortedTransactionRange> aborted;
            if (read_committed && p->aborted_transactions)
                for (const auto & txn : *p->aborted_transactions)
                    aborted.push_back({txn.producer_id, txn.first_offset});
            auto assembled = Protocol::assembleFetchedRecords(*p->records, spec.offset, read_committed, std::move(aborted));
            result.records = std::move(assembled.records);
            result.next_offset = assembled.next_offset;
            result.truncated_tail = assembled.truncated;
        }
        return result;
    });
}

Int64 listOffset(Cluster & cluster, const TopicPartition & tp, OffsetSpec spec, const Deadline & deadline)
{
    return withLeaderRetries(cluster, tp, deadline, [&](const LeaderRef & leader)
    {
        auto lease = cluster.acquire(leader.node_id, leader.address, deadline);

        Protocol::ListOffsetsRequest request;
        /// Under read_committed, "latest" means the last stable offset.
        request.isolation_level = cluster.getConfig().isolation_level == IsolationLevel::ReadCommitted ? 1 : 0;
        Protocol::ListOffsetsPartition partition;
        partition.partition_index = tp.partition;
        partition.current_leader_epoch = leader.leader_epoch;
        partition.timestamp = spec.wireTimestamp();
        request.topics = {{tp.topic, {partition}}};

        const auto response = lease->call(request, deadline);
        const Protocol::ListOffsetsPartitionResponse * p = nullptr;
        for (const auto & topic : response.topics)
            if (topic.name == tp.topic)
                for (const auto & part : topic.partitions)
                    if (part.partition_index == tp.partition)
                        p = &part;
        if (!p)
            throw Exception(ErrorKind::Protocol, "List offsets response misses the requested partition");
        if (p->error_code != BrokerError::NONE)
            throw Exception(BrokerCode{p->error_code}, "List offsets for {}[{}] failed", tp.topic, tp.partition);
        return p->offset;
    });
}

ClusterMetadata getMetadata(Cluster & cluster, const std::vector<String> & topics, const Deadline & deadline)
{
    std::vector<Protocol::MetadataTopic> raw_topics;
    raw_topics.reserve(topics.size());
    if (topics.empty())
    {
        /// Brokers-only refresh (empty topic array = no topics).
        cluster.refreshMetadata({}, false, false, deadline);
    }
    else
    {
        for (const auto & topic : topics)
            raw_topics.push_back(cluster.topicMetadata(topic, deadline));
    }

    ClusterMetadata out;
    const auto brokers = cluster.knownBrokers();
    out.brokers = brokers.brokers;
    out.cluster_id = brokers.cluster_id;
    out.controller_id = brokers.controller_id;
    for (const auto & raw : raw_topics)
    {
        TopicMetadata topic;
        topic.name = raw.name.value_or("");
        topic.topic_id = raw.topic_id;
        topic.is_internal = raw.is_internal;
        for (const auto & p : raw.partitions)
        {
            PartitionMetadata partition;
            partition.partition = p.partition_index;
            partition.leader_id = p.leader_id;
            partition.leader_epoch = p.leader_epoch;
            partition.replicas = p.replica_nodes;
            partition.isr = p.isr_nodes;
            topic.partitions.push_back(std::move(partition));
        }
        std::sort(
            topic.partitions.begin(), topic.partitions.end(), [](const auto & a, const auto & b) { return a.partition < b.partition; });
        out.topics.push_back(std::move(topic));
    }
    return out;
}

namespace
{

/// Coordinator-flavoured retry loop: invalidate the cached coordinator and
/// retry on NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE (fresh groups
/// bootstrap __consumer_offsets lazily) and on connection loss.
template <typename Op>
auto withCoordinatorRetries(Cluster & cluster, const String & group, const Deadline & deadline, Op && op)
{
    for (int attempt = 1;; ++attempt)
    {
        try
        {
            auto lease = cluster.acquireCoordinator(group, deadline);
            return op(lease);
        }
        catch (const Exception & e)
        {
            const bool coordinator_stale = e.getKind() == ErrorKind::Broker
                && (BrokerError::isCoordinatorStale(e.getBrokerCode())
                    || e.getBrokerCode() == BrokerError::COORDINATOR_LOAD_IN_PROGRESS);
            const bool connection_lost = e.getKind() == ErrorKind::Connection;
            /// Coordinator bootstrap (a fresh group creating its
            /// __consumer_offsets partition) can take a while — retry those
            /// until the deadline; cap only connection-loss retries.
            const int attempt_budget = coordinator_stale ? 50 : max_attempts;
            if ((!coordinator_stale && !connection_lost) || attempt >= attempt_budget || deadline.expired())
                throw;
            LOG_DEBUG(cluster.getLogger(), "Group '{}': attempt {} failed ({}), retrying", group, attempt, e.message());
            cluster.invalidateCoordinator(group);
            cluster.backoff(retry_backoff, deadline);
        }
    }
}

}

void commitOffsets(Cluster & cluster, const String & group, const std::vector<TopicPartitionOffset> & offsets, const Deadline & deadline)
{
    if (offsets.empty())
        return;

    Protocol::OffsetCommitRequest request;
    request.group_id = group;
    request.generation_id_or_member_epoch = -1; // group-less: no membership
    request.member_id = "";
    std::map<String, Protocol::OffsetCommitTopic> topics;
    for (const auto & offset : offsets)
    {
        Protocol::OffsetCommitPartition partition;
        partition.partition_index = offset.partition;
        partition.committed_offset = offset.offset;
        partition.committed_leader_epoch = offset.leader_epoch;
        partition.committed_metadata = offset.metadata;
        auto & topic = topics[offset.topic];
        topic.name = offset.topic;
        topic.partitions.push_back(std::move(partition));
    }
    for (auto & [name, topic] : topics)
        request.topics.push_back(std::move(topic));

    withCoordinatorRetries(cluster, group, deadline, [&](Cluster::Lease & lease)
    {
        const auto response = lease->call(request, deadline);
        for (const auto & topic : response.topics)
            for (const auto & partition : topic.partitions)
                if (partition.error_code != BrokerError::NONE)
                    throw Exception(BrokerCode{partition.error_code}, "Offset commit for {}[{}] in group '{}' failed",
                        topic.name,
                        partition.partition_index,
                        group);
    });
}

std::vector<TopicPartitionOffset> fetchCommittedOffsets(
    Cluster & cluster, const String & group, const std::vector<TopicPartition> & partitions, const Deadline & deadline)
{
    Protocol::OffsetFetchRequest request;
    Protocol::OffsetFetchGroup request_group;
    request_group.group_id = group;
    request_group.topics.emplace();
    std::map<String, Protocol::OffsetFetchTopic> topics;
    for (const auto & tp : partitions)
    {
        auto & topic = topics[tp.topic];
        topic.name = tp.topic;
        topic.partition_indexes.push_back(tp.partition);
    }
    for (auto & [name, topic] : topics)
        request_group.topics->push_back(std::move(topic));
    request.groups = {std::move(request_group)};

    return withCoordinatorRetries(cluster, group, deadline, [&](Cluster::Lease & lease)
    {
        const auto response = lease->call(request, deadline);
        if (response.groups.size() != 1)
            throw Exception(ErrorKind::Protocol, "Offset fetch response group count mismatch");
        const auto & group_response = response.groups[0];
        if (group_response.error_code != BrokerError::NONE)
            throw Exception(BrokerCode{group_response.error_code}, "Offset fetch for group '{}' failed", group);

        std::vector<TopicPartitionOffset> out;
        out.reserve(partitions.size());
        for (const auto & tp : partitions)
        {
            const Protocol::OffsetFetchPartitionResponse * found = nullptr;
            for (const auto & topic : group_response.topics)
                if (topic.name == tp.topic)
                    for (const auto & partition : topic.partitions)
                        if (partition.partition_index == tp.partition)
                            found = &partition;
            if (!found)
                throw Exception(ErrorKind::Protocol, "Offset fetch response misses {}[{}]", tp.topic, tp.partition);
            if (found->error_code != BrokerError::NONE)
                throw Exception(BrokerCode{found->error_code}, "Offset fetch for {}[{}] in group '{}' failed",
                    tp.topic,
                    tp.partition,
                    group);
            TopicPartitionOffset offset;
            offset.topic = tp.topic;
            offset.partition = tp.partition;
            offset.offset = found->committed_offset;
            offset.leader_epoch = found->committed_leader_epoch;
            offset.metadata = found->metadata;
            out.push_back(std::move(offset));
        }
        return out;
    });
}

Client::ProduceResult produce(Cluster & cluster, const Client::ProduceSpec & spec, const Deadline & deadline)
{
    if (spec.records.empty())
        throw Exception(ErrorKind::Config, "Produce requires at least one record");
    if (spec.acks != -1 && spec.acks != 0 && spec.acks != 1)
        throw Exception(ErrorKind::Config, "The acks value must be -1, 0 or 1");

    /// Build one uncompressed batch. Absolute timestamps become base + delta;
    /// -1 means "now".
    const Int64 now_ms
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::vector<Protocol::BatchRecord> records;
    records.reserve(spec.records.size());
    const Int64 first_timestamp = spec.records.front().timestamp_ms >= 0 ? spec.records.front().timestamp_ms : now_ms;
    Int64 max_timestamp = first_timestamp;
    for (size_t i = 0; i < spec.records.size(); ++i)
    {
        const auto & in = spec.records[i];
        const Int64 timestamp = in.timestamp_ms >= 0 ? in.timestamp_ms : now_ms;
        max_timestamp = std::max(max_timestamp, timestamp);
        Protocol::BatchRecord record;
        record.offset_delta = static_cast<Int32>(i);
        record.timestamp_delta = timestamp - first_timestamp;
        record.key = in.key;
        record.value = in.value;
        record.headers = in.headers;
        records.push_back(std::move(record));
    }
    auto batch = Protocol::RecordBatch::make(0, std::move(records));
    batch.first_timestamp = first_timestamp;
    batch.max_timestamp = max_timestamp;

    /// Compression values match the wire encoding (see Types.h), so the public
    /// enum maps onto the codec bits directly.
    static_assert(static_cast<int>(Compression::Gzip) == static_cast<int>(Protocol::CompressionType::Gzip));
    static_assert(static_cast<int>(Compression::Zstd) == static_cast<int>(Protocol::CompressionType::Zstd));
    /// The request only holds a view of the blob; keep it alive for the call.
    const String blob = Protocol::encodeBatches(
        {batch}, static_cast<Protocol::CompressionType>(spec.compression), spec.compression_level);

    Protocol::ProduceRequest request;
    request.acks = spec.acks;
    Protocol::ProducePartitionData partition;
    partition.index = spec.partition;
    partition.records = blob;
    request.topic_data = {{spec.topic, {partition}}};

    const TopicPartition tp{spec.topic, spec.partition};
    /// NOTE: a retry after a connection error can duplicate records — there is
    /// no idempotent producer (KIP-98) in v1.
    return withLeaderRetries(cluster, tp, deadline, [&](const LeaderRef & leader)
    {
        auto lease = cluster.acquire(leader.node_id, leader.address, deadline);
        request.timeout_ms = static_cast<Int32>(
            std::max<Int64>(1'000, deadline.remainingClamped(std::chrono::milliseconds{30'000}).count()));

        if (spec.acks == 0)
        {
            lease->callNoResponse(request, deadline);
            return Client::ProduceResult{};
        }

        const auto response = lease->call(request, deadline);
        const Protocol::ProducePartitionResponse * p = nullptr;
        for (const auto & topic : response.responses)
            if (topic.name == spec.topic)
                for (const auto & part : topic.partition_responses)
                    if (part.index == spec.partition)
                        p = &part;
        if (!p)
            throw Exception(ErrorKind::Protocol, "Produce response misses the requested partition");
        if (p->error_code != BrokerError::NONE)
            throw Exception(BrokerCode{p->error_code}, "Produce to {}[{}] failed{}",
                spec.topic,
                spec.partition,
                p->error_message ? ": " + *p->error_message : String{});
        Client::ProduceResult result;
        result.base_offset = p->base_offset;
        result.log_append_time_ms = p->log_append_time_ms;
        return result;
    });
}

void ensureTopic(Cluster & cluster, const String & topic, const Deadline & deadline)
{
    for (;;)
    {
        /// Auto-created topics return zero partitions (or transient
        /// UNKNOWN_TOPIC / LEADER_NOT_AVAILABLE) on the first responses while
        /// creation runs asynchronously — poll until partitions appear.
        cluster.refreshMetadata({topic}, true, true, deadline);
        const auto cached = cluster.cachedTopic(topic);
        if (cached && !cached->partitions.empty())
            return;
        cluster.invalidateTopic(topic);
        if (deadline.expired())
            throw Exception(ErrorKind::Timeout, "Topic '{}' did not become ready in time", topic);
        cluster.backoff(std::chrono::milliseconds{100}, deadline);
    }
}

}
