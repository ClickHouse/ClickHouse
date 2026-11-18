#pragma once

#include <Server/Kafka/KafkaProtocol.h>

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

#include <functional>
#include <map>
#include <memory>
#include <vector>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
class MergeTreeData;
}

/** A Kafka topic is a MergeTree table with the persistent `_block_number` and `_block_offset` columns
  * (the table settings `enable_block_number_column` and `enable_block_offset_column`).
  *
  * The table stores the records in the columns `value`, and optionally `key`, `timestamp` and `headers`.
  * An unpartitioned table is a topic with one partition. A table partitioned by a single integer column
  * is a topic with the partitions numbered by the values of this column. The number of partitions is taken
  * from a constraint `CHECK <column> < N`, or from the largest existing partition otherwise.
  *
  * The offset of a record is `_block_number << 32 | _block_offset`: the offsets are ascending in the
  * order of commit, and have gaps between blocks, as in a compacted Kafka topic. Every inserted record
  * batch is one block. The records are read with a streaming query (`FROM table STREAM`), which only
  * returns the committed prefix of blocks of every partition, so the offsets never go backwards
  * even with concurrent inserts into a `ReplicatedMergeTree`.
  */
namespace DB::Kafka
{

struct BrokerSettings
{
    /// The database of the topics with unqualified names. A topic `db.table` is a table in the database `db`, if it exists.
    String database = "default";

    /// The host and port to return to clients in the metadata. By default, the address the client connected to.
    /// `advertised_port_secure` is returned to the clients connected to `kafka_port_secure`.
    String advertised_host;
    UInt16 advertised_port = 0;
    UInt16 advertised_port_secure = 0;

    String cluster_id = "ClickHouse";

    /// Whether to write the queries made for Kafka requests to `system.query_log`.
    bool log_queries = false;

    UInt64 max_request_size = 100 * 1024 * 1024;

    /// The delay of the first rebalance of a group, to let more members join, as `group.initial.rebalance.delay.ms` of Kafka.
    UInt64 group_initial_rebalance_delay_ms = 3000;

    /// The table with the committed offsets of consumer groups, created on the first commit.
    String offsets_table = "__consumer_offsets";

    /// The table engine of the tables created by `CreateTopics`.
    String topic_engine = "MergeTree";

    /// The format of the messages of the tables without a column `value` and without the table setting `kafka_format`.
    String format = "JSONEachRow";

    /// Whether a Metadata request creates the missing topics, if the client allows it, as `auto.create.topics.enable` of Kafka.
    bool auto_create_topics = false;
    Int32 default_num_partitions = 1;

    void load(const Poco::Util::AbstractConfiguration & config, const String & prefix);
};

using QueryContextFactory = std::function<ContextMutablePtr()>;

struct Topic
{
    String name;
    StorageID table_id = StorageID::createEmpty();
    StoragePtr storage;
    MergeTreeData * data = nullptr;

    /// Empty for an unpartitioned table.
    String partition_column;
    Int32 num_partitions = 1;

    DataTypePtr key_type;
    DataTypePtr value_type;
    DataTypePtr timestamp_type;
    DataTypePtr headers_type;

    /// Non-empty if the rows are serialized into the messages in this format; then `value_type` is null,
    /// and the message contains `value_columns`.
    String format;
    bool binary_format = false;
    NamesAndTypesList value_columns;

    String partitionId(Int32 partition) const;
    std::optional<Int32> partitionIndex(const String & partition_id) const;
    bool hasPartition(Int32 partition) const { return partition >= 0 && partition < num_partitions; }
};

using TopicPtr = std::shared_ptr<const Topic>;

struct TopicResolution
{
    TopicPtr topic;
    ErrorCode error = ErrorCode::NONE;
    String error_message;
};

TopicResolution resolveTopic(const String & name, const BrokerSettings & settings, ContextPtr context);

/// The names of the topics in the default database, which the user is allowed to see.
std::vector<String> listTopics(const BrokerSettings & settings, ContextPtr context);

constexpr Int64 makeOffset(UInt64 block_number, UInt64 block_offset)
{
    return static_cast<Int64>((block_number << 32) | block_offset);
}

struct PartitionState
{
    Int64 log_start_offset = 0;
    Int64 high_watermark = 0;
    /// The highest block number up to which all blocks are committed, -1 if none.
    Int64 safe_block_number = -1;
};

/// The current committed bounds of the partitions of a topic.
std::map<Int32, PartitionState> getPartitionStates(const Topic & topic, const std::vector<Int32> & partitions, const QueryContextFactory & make_context);

/// The highest block numbers of the committed prefix of every partition, by partition id.
std::map<String, Int64> getSafeBlockNumbers(const Topic & topic);

struct FetchPartitionRequest
{
    Int32 partition = 0;
    Int64 offset = 0;
    Int32 max_bytes = 0;
};

struct FetchPartitionResult
{
    Int32 partition = 0;
    ErrorCode error = ErrorCode::NONE;
    Int64 high_watermark = -1;
    Int64 log_start_offset = -1;
    Int64 safe_block_number = -1;
    /// Encoded record batches.
    String records;
    size_t num_records = 0;
};

/// Reads the records of the partitions from the requested offsets, at most `max_bytes` in total (but at least one record).
std::vector<FetchPartitionResult> fetch(
    const Topic & topic,
    const std::vector<FetchPartitionRequest> & requests,
    size_t max_bytes,
    const QueryContextFactory & make_context);

/// Inserts the record batches into one partition as one block per batch.
/// For an idempotent producer, the batch is deduplicated by its producer id, epoch and sequence number.
void produce(
    const Topic & topic,
    Int32 partition,
    const std::vector<RecordBatch> & batches,
    const QueryContextFactory & make_context);

struct OffsetForTimestamp
{
    Int64 offset = -1;
    Int64 timestamp = -1;
};

/// The first offset with the timestamp greater or equal to the given one, as the ListOffsets request.
/// The timestamp -1 means the latest offset, -2 the earliest, -3 the record with the largest timestamp.
OffsetForTimestamp getOffsetForTimestamp(const Topic & topic, Int32 partition, Int64 timestamp, const QueryContextFactory & make_context);

/// Creates a table for a topic.
void createTopic(
    const String & name,
    Int32 num_partitions,
    const std::vector<std::pair<String, std::optional<String>>> & configs,
    bool validate_only,
    const BrokerSettings & settings,
    const QueryContextFactory & make_context);

/// Drops the table of a topic. The caller must not hold a reference to the table, because the drop waits for all of them to be released.
void deleteTopic(const StorageID & table_id, const QueryContextFactory & make_context);

/// Runs a query and passes the blocks of its result to the callback, which returns false to stop the query.
void runSelect(const String & query, ContextMutablePtr context, const std::function<bool(const Block &)> & on_block);

/// Runs a query with no result.
void runQuery(const String & query, ContextMutablePtr context);

/// Runs an INSERT query with the data after the query text.
void runQueryWithData(const String & query_with_data, ContextMutablePtr context);

}
