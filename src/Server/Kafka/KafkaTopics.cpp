#include <Server/Kafka/KafkaTopics.h>
#include <Server/Kafka/KafkaError.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/QueryScope.h>
#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/NativeWriter.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageProxy.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <mutex>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
    extern const MergeTreeSettingsString kafka_format;
}

namespace Kafka
{

void BrokerSettings::load(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    database = config.getString(prefix + ".database", database);
    advertised_host = config.getString(prefix + ".advertised_host", advertised_host);
    advertised_port = static_cast<UInt16>(config.getUInt(prefix + ".advertised_port", advertised_port));
    advertised_port_secure = static_cast<UInt16>(config.getUInt(prefix + ".advertised_port_secure", advertised_port_secure));
    cluster_id = config.getString(prefix + ".cluster_id", cluster_id);
    log_queries = config.getBool(prefix + ".log_queries", log_queries);
    max_request_size = config.getUInt64(prefix + ".max_request_size", max_request_size);
    group_initial_rebalance_delay_ms = config.getUInt64(prefix + ".group_initial_rebalance_delay_ms", group_initial_rebalance_delay_ms);
    offsets_table = config.getString(prefix + ".offsets_table", offsets_table);
    topic_engine = config.getString(prefix + ".topic_engine", topic_engine);
    format = config.getString(prefix + ".format", format);
    auto_create_topics = config.getBool(prefix + ".auto_create_topics", auto_create_topics);
    default_num_partitions = config.getInt(prefix + ".default_num_partitions", default_num_partitions);
}


String Topic::partitionId(Int32 partition) const
{
    if (partition_column.empty())
        return "all";
    return std::to_string(partition);
}

std::optional<Int32> Topic::partitionIndex(const String & partition_id) const
{
    if (partition_column.empty())
    {
        if (partition_id == "all")
            return 0;
        return std::nullopt;
    }

    Int32 partition = 0;
    if (!tryParse(partition, partition_id) || std::to_string(partition) != partition_id || partition < 0)
        return std::nullopt;
    return partition;
}


namespace
{

TopicResolution makeTopic(
    const String & name, const StorageID & table_id, StoragePtr storage, const BrokerSettings & settings, DeferredTable deferred_table)
{
    TopicResolution res;
    auto fail = [&](String message)
    {
        res.error = ErrorCode::INVALID_TOPIC_EXCEPTION;
        res.error_message = std::move(message);
        return res;
    };

    /// The raw pointer stays valid while `topic->storage` holds the table, including through a proxy.
    auto * data = castStorage<MergeTreeData>(storage, deferred_table).get();
    if (!data || !storage->supportsStreaming())
        return fail(fmt::format("Table {} is not a MergeTree table", table_id.getNameForLogs()));

    const auto merge_tree_settings = data->getSettings();
    if (!(*merge_tree_settings)[MergeTreeSetting::enable_block_number_column] || !(*merge_tree_settings)[MergeTreeSetting::enable_block_offset_column])
        return fail(fmt::format("Table {} must have the settings `enable_block_number_column = 1` and `enable_block_offset_column = 1` to be a Kafka topic", table_id.getNameForLogs()));

    auto topic = std::make_shared<Topic>();
    topic->name = name;
    topic->table_id = table_id;
    topic->storage = storage;
    topic->data = data;

    auto metadata = storage->getInMemoryMetadataPtr(/* context = */ nullptr, /* bypass_metadata_cache = */ false);
    const auto & columns = metadata->getColumns();
    auto get_type = [&](const String & column_name) -> DataTypePtr
    {
        auto column = columns.tryGetPhysical(column_name);
        return column ? column->type : nullptr;
    };

    topic->key_type = get_type("key");
    topic->value_type = get_type("value");
    topic->timestamp_type = get_type("timestamp");
    topic->headers_type = get_type("headers");

    const auto & partition_key = metadata->getPartitionKey();
    if (!partition_key.column_names.empty())
    {
        const ASTIdentifier * identifier = nullptr;
        if (partition_key.expression_list_ast && partition_key.expression_list_ast->children.size() == 1)
            identifier = partition_key.expression_list_ast->children[0]->as<ASTIdentifier>();

        if (!identifier || partition_key.data_types.size() != 1 || !isInteger(partition_key.data_types[0]))
            return fail(fmt::format("Table {} must be unpartitioned or partitioned by a single integer column to be a Kafka topic", table_id.getNameForLogs()));

        topic->partition_column = identifier->name();

        Int64 num_partitions = 1;
        for (const auto & constraint_ast : metadata->getConstraints().getConstraints())
        {
            const auto * constraint = constraint_ast->as<ASTConstraintDeclaration>();
            if (!constraint || constraint->type != ASTConstraintDeclaration::Type::CHECK || !constraint->expr)
                continue;

            const auto * function = constraint->expr->as<ASTFunction>();
            if (!function || function->name != "less" || !function->arguments || function->arguments->children.size() != 2)
                continue;

            const auto * column = function->arguments->children[0]->as<ASTIdentifier>();
            const auto * literal = function->arguments->children[1]->as<ASTLiteral>();
            if (!column || !literal || column->name() != topic->partition_column || literal->value.getType() != Field::Types::UInt64)
                continue;

            num_partitions = std::max<Int64>(num_partitions, std::min<UInt64>(literal->value.safeGet<UInt64>(), std::numeric_limits<Int32>::max()));
        }

        for (const auto & part : data->getDataPartsVectorForInternalUsage())
            if (auto partition = topic->partitionIndex(part->info.getPartitionId()))
                num_partitions = std::max<Int64>(num_partitions, static_cast<Int64>(*partition) + 1);

        topic->num_partitions = static_cast<Int32>(num_partitions);
    }

    /// A table without the column `value`, or with the setting `kafka_format`, has its rows serialized in the messages.
    String format = (*merge_tree_settings)[MergeTreeSetting::kafka_format];
    if (format.empty() && !(topic->value_type && isStringOrFixedString(removeNullable(removeLowCardinality(topic->value_type)))))
        format = settings.format;

    if (!format.empty())
    {
        const auto & format_factory = FormatFactory::instance();
        if (!format_factory.isInputFormat(format) || !format_factory.isOutputFormat(format))
            return fail(fmt::format("Format {} of the table {} is not an input and output format", format, table_id.getNameForLogs()));

        topic->format = format;
        topic->binary_format = format_factory.getContentType(format, std::nullopt) == "application/octet-stream";
        topic->value_type = nullptr;

        for (const auto & column : columns.getOrdinary())
        {
            if (column.name == topic->partition_column || column.name == "key" || column.name == "timestamp" || column.name == "headers")
                continue;
            topic->value_columns.push_back(column);
        }

        if (topic->value_columns.empty())
            return fail(fmt::format("Table {} has no columns for the messages", table_id.getNameForLogs()));
    }

    res.topic = std::move(topic);
    return res;
}

/// The number of rows of a block is cached, because the high watermark is computed on every Fetch request.
class LastBlockRowsCache
{
public:
    std::optional<UInt64> get(const String & key, Int64 block_number)
    {
        std::lock_guard lock(mutex);
        auto it = cache.find(key);
        if (it == cache.end() || it->second.first != block_number)
            return std::nullopt;
        return it->second.second;
    }

    void set(const String & key, Int64 block_number, UInt64 rows)
    {
        std::lock_guard lock(mutex);
        cache[key] = {block_number, rows};
    }

    static LastBlockRowsCache & instance()
    {
        static LastBlockRowsCache res;
        return res;
    }

private:
    std::mutex mutex;
    std::unordered_map<String, std::pair<Int64, UInt64>> cache;
};

String tableName(const Topic & topic)
{
    return topic.table_id.getFullTableName();
}

/// The number of the next offset in the block, i.e. the max `_block_offset` + 1.
UInt64 getBlockEndOffset(const Topic & topic, const String & partition_id, Int64 block_number, const QueryContextFactory & make_context)
{
    String cache_key = fmt::format("{}\n{}\n{}", toString(topic.storage->getStorageID().uuid), tableName(topic), partition_id);
    if (auto cached = LastBlockRowsCache::instance().get(cache_key, block_number))
        return *cached;

    UInt64 result = 0;
    runSelect(
        fmt::format(
            "SELECT count(), max(_block_offset) FROM {} WHERE _partition_id = {} AND _block_number = {}",
            tableName(topic), quoteString(partition_id), block_number),
        make_context(),
        [&](const Block & block)
        {
            if (block.rows() && block.getByPosition(0).column->getUInt(0))
                result = block.getByPosition(1).column->getUInt(0) + 1;
            return true;
        });

    LastBlockRowsCache::instance().set(cache_key, block_number, result);
    return result;
}

}


TopicResolution resolveTopic(const String & name, const BrokerSettings & settings, ContextPtr context)
{
    TopicResolution res;
    if (name.empty() || name.size() > 249)
    {
        res.error = ErrorCode::INVALID_TOPIC_EXCEPTION;
        res.error_message = "Invalid topic name";
        return res;
    }

    StorageID table_id(settings.database, name);
    if (auto dot = name.find('.'); dot != String::npos && dot > 0 && dot + 1 < name.size()
        && DatabaseCatalog::instance().isDatabaseExist(name.substr(0, dot)))
        table_id = StorageID(name.substr(0, dot), name.substr(dot + 1));

    if (!context->getAccess()->isGranted(AccessType::SHOW_TABLES, table_id.database_name, table_id.table_name))
    {
        res.error = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
        res.error_message = fmt::format("Not enough privileges to access the table {}", table_id.getNameForLogs());
        return res;
    }

    auto storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
    if (!storage)
    {
        res.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
        res.error_message = fmt::format("Table {} does not exist", table_id.getNameForLogs());
        return res;
    }

    return makeTopic(name, storage->getStorageID(), storage, settings, DeferredTable::Load);
}

std::vector<String> listTopics(const BrokerSettings & settings, ContextPtr context)
{
    std::vector<String> res;
    auto database = DatabaseCatalog::instance().tryGetDatabase(settings.database);
    if (!database)
        return res;

    const auto access = context->getAccess();
    for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, settings.database, it->name()))
            continue;

        auto storage = it->table();
        if (!storage)
            continue;

        if (makeTopic(it->name(), storage->getStorageID(), storage, settings, DeferredTable::Skip).topic)
            res.push_back(it->name());
    }

    std::sort(res.begin(), res.end());
    return res;
}


std::map<String, Int64> getSafeBlockNumbers(const Topic & topic)
{
    LocalPartsByPartition local_parts;
    for (const auto & part : topic.data->getDataPartsVectorForInternalUsage())
        local_parts[part->info.getPartitionId()].push_back(part->info);

    auto promoters = topic.data->buildPromoters();
    MergeTreeBoundsSubscription subscription(1, 0);
    enrichSubscription(subscription, local_parts, promoters);
    return subscription.snapshot().safe_block_numbers;
}

std::map<Int32, PartitionState> getPartitionStates(const Topic & topic, const std::vector<Int32> & partitions, const QueryContextFactory & make_context)
{
    std::map<String, Int64> min_block_numbers;
    for (const auto & part : topic.data->getDataPartsVectorForInternalUsage())
    {
        auto [it, inserted] = min_block_numbers.emplace(part->info.getPartitionId(), part->info.min_block);
        if (!inserted)
            it->second = std::min(it->second, part->info.min_block);
    }

    const auto safe_block_numbers = getSafeBlockNumbers(topic);

    std::map<Int32, PartitionState> res;
    for (Int32 partition : partitions)
    {
        if (res.contains(partition))
            continue;

        PartitionState & state = res[partition];
        const String partition_id = topic.partitionId(partition);

        if (auto it = min_block_numbers.find(partition_id); it != min_block_numbers.end())
            state.log_start_offset = makeOffset(it->second, 0);

        state.high_watermark = state.log_start_offset;
        if (auto it = safe_block_numbers.find(partition_id); it != safe_block_numbers.end() && it->second >= 0)
        {
            state.safe_block_number = it->second;
            UInt64 end_offset = getBlockEndOffset(topic, partition_id, it->second, make_context);
            state.high_watermark = std::max(state.high_watermark, makeOffset(it->second, end_offset));
        }
    }
    return res;
}


namespace
{

/// The upper bound of block numbers to read in one Fetch for a partition, to read roughly `max_bytes` of data
/// instead of the whole remainder of the partition. Estimated from the sizes of the parts.
Int64 chooseBlockNumberBound(const Topic & topic, const String & partition_id, Int64 start_block, Int64 safe_block_number, size_t max_bytes)
{
    struct PartRange
    {
        Int64 min_block;
        Int64 max_block;
        UInt64 bytes;
    };

    std::vector<PartRange> ranges;
    for (const auto & part : topic.data->getDataPartsVectorForInternalUsage())
    {
        if (part->info.getPartitionId() != partition_id || part->info.max_block < start_block || part->info.min_block > safe_block_number)
            continue;
        ranges.push_back({part->info.min_block, part->info.max_block, std::max<UInt64>(1, part->getBytesUncompressedOnDisk())});
    }
    std::sort(ranges.begin(), ranges.end(), [](const auto & a, const auto & b) { return a.min_block < b.min_block; });

    UInt64 accumulated = 0;
    for (const auto & range : ranges)
    {
        Int64 from = std::max(range.min_block, start_block);
        Int64 span = range.max_block - range.min_block + 1;
        UInt64 bytes_from = static_cast<UInt64>(static_cast<double>(range.bytes) * static_cast<double>(range.max_block - from + 1) / static_cast<double>(span));

        if (accumulated + bytes_from >= max_bytes)
        {
            /// Interpolate inside the part, assuming the blocks are of equal size.
            double bytes_per_block = static_cast<double>(range.bytes) / static_cast<double>(span);
            Int64 blocks_needed = static_cast<Int64>(static_cast<double>(max_bytes - accumulated) / bytes_per_block) + 1;
            return std::min(safe_block_number, from + blocks_needed - 1);
        }
        accumulated += bytes_from;
    }

    return safe_block_number;
}

struct PendingRead
{
    size_t result_index = 0;
    String partition_id;
    Int64 start_offset = 0;
    Int64 high_watermark = 0;
    Int64 safe_block_number = -1;
    /// The upper bound of `_block_number` of the next read.
    Int64 bound = -1;
    size_t max_bytes = 0;
    size_t bytes = 0;
    bool full = false;
    std::vector<Record> records;
};

String nullableStringExpression(const DataTypePtr & type, const String & column)
{
    if (!type)
        return "CAST(NULL AS Nullable(String))";
    return fmt::format("CAST({} AS Nullable(String))", backQuote(column));
}

/// The value of a message: the column `value`, or the row serialized in the format of the topic.
String valueExpression(const Topic & topic)
{
    if (topic.format.empty())
        return nullableStringExpression(topic.value_type, "value");

    String arguments;
    for (const auto & column : topic.value_columns)
        arguments += ", " + backQuote(column.name);

    /// A text format ends a row with a newline, which is not a part of the message. A binary format may end with this byte.
    return fmt::format("CAST({}({}{}) AS Nullable(String))",
        topic.binary_format ? "formatRow" : "formatRowNoNewline", quoteString(topic.format), arguments);
}

void appendBatches(FetchPartitionResult & result, std::vector<Record> & records)
{
    size_t begin = 0;
    while (begin < records.size())
    {
        /// One batch per block: the offset deltas inside it fit into 32 bits.
        size_t end = begin + 1;
        while (end < records.size() && (records[end].offset >> 32) == (records[begin].offset >> 32))
            ++end;

        std::vector<Record> batch(std::make_move_iterator(records.begin() + begin), std::make_move_iterator(records.begin() + end));
        writeRecordBatch(result.records, batch);
        result.num_records += batch.size();
        begin = end;
    }
}

/// Reads the records of the partitions from their start offsets up to their bounds of block numbers with one streaming query.
/// Returns false if the read was stopped because `max_bytes` were read.
bool readPartitions(
    const Topic & topic,
    const std::vector<PendingRead *> & reads,
    size_t max_bytes,
    size_t & total_bytes,
    const QueryContextFactory & make_context)
{
    String cursor;
    String filter;
    std::unordered_map<String, PendingRead *> reads_by_partition_id;

    for (PendingRead * read : reads)
    {
        reads_by_partition_id[read->partition_id] = read;

        const Int64 block_number = read->start_offset >> 32;
        const Int64 block_offset = read->start_offset & 0xFFFFFFFF;

        /// The cursor is the position of the last read record.
        if (!cursor.empty())
            cursor += ", ";
        if (block_offset > 0)
            cursor += fmt::format("{}: {{'block_number': {}, 'block_offset': {}}}", quoteString(read->partition_id), block_number, block_offset - 1);
        else
            cursor += fmt::format("{}: {{'block_number': {}}}", quoteString(read->partition_id), block_number);

        if (!filter.empty())
            filter += " OR ";
        filter += fmt::format("(_partition_id = {} AND _block_number <= {})", quoteString(read->partition_id), read->bound);
    }

    String timestamp_expression = "-1";
    if (topic.timestamp_type)
        timestamp_expression = fmt::format("ifNull(toUnixTimestamp64Milli(CAST({} AS Nullable(DateTime64(3)))), -1)", backQuote("timestamp"));

    String headers_expression = "CAST(map() AS Map(String, String))";
    if (topic.headers_type)
        headers_expression = fmt::format("CAST({} AS Map(String, String))", backQuote("headers"));

    String query = fmt::format(
        "SELECT _partition_id, _block_number, _block_offset, {}, {}, {}, {} FROM {} STREAM BOUNDED CURSOR {{{}}} WHERE {}",
        nullableStringExpression(topic.key_type, "key"),
        valueExpression(topic),
        timestamp_expression,
        headers_expression,
        tableName(topic),
        cursor,
        filter);

    size_t num_full = 0;
    bool stopped_by_limit = false;

    runSelect(query, make_context(), [&](const Block & original_block)
    {
        Block block = original_block;
        removeSpecialColumnRepresentations(block);
        /// The missing columns of a record are constants, e.g. `map()`.
        for (auto & column : block)
            column.column = column.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

        const auto & partition_ids = *block.getByPosition(0).column;
        const auto & block_numbers = *block.getByPosition(1).column;
        const auto & block_offsets = *block.getByPosition(2).column;
        const auto & keys = assert_cast<const ColumnNullable &>(*block.getByPosition(3).column);
        const auto & values = assert_cast<const ColumnNullable &>(*block.getByPosition(4).column);
        const auto & timestamps = *block.getByPosition(5).column;
        const auto & headers = assert_cast<const ColumnMap &>(*block.getByPosition(6).column);
        const auto & headers_offsets = headers.getNestedColumn().getOffsets();
        const auto & header_keys = headers.getNestedData().getColumn(0);
        const auto & header_values = headers.getNestedData().getColumn(1);

        for (size_t row = 0; row < block.rows(); ++row)
        {
            auto it = reads_by_partition_id.find(String(partition_ids.getDataAt(row)));
            if (it == reads_by_partition_id.end() || it->second->full)
                continue;

            PendingRead & read = *it->second;
            UInt64 block_offset = block_offsets.getUInt(row);
            if (block_offset > 0xFFFFFFFF)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The block offset {} does not fit into a Kafka offset", block_offset);

            Int64 offset = makeOffset(block_numbers.getUInt(row), block_offset);
            if (offset < read.start_offset || offset >= read.high_watermark || (!read.records.empty() && offset <= read.records.back().offset))
                continue;

            Record & record = read.records.emplace_back();
            record.offset = offset;
            record.timestamp = timestamps.getInt(row);
            if (!keys.isNullAt(row))
                record.key = String(keys.getNestedColumn().getDataAt(row));
            if (!values.isNullAt(row))
                record.value = String(values.getNestedColumn().getDataAt(row));

            size_t record_bytes = 16 + (record.key ? record.key->size() : 0) + (record.value ? record.value->size() : 0);
            for (size_t j = headers_offsets[row - 1]; j < headers_offsets[row]; ++j)
            {
                record.headers.emplace_back(String(header_keys.getDataAt(j)), String(header_values.getDataAt(j)));
                record_bytes += 2 + record.headers.back().first.size() + record.headers.back().second->size();
            }

            read.bytes += record_bytes;
            total_bytes += record_bytes;
            if (read.bytes >= read.max_bytes)
            {
                read.full = true;
                ++num_full;
            }

            if (total_bytes >= max_bytes)
            {
                stopped_by_limit = true;
                return false;
            }
        }

        return num_full < reads.size();
    });

    return !stopped_by_limit;
}

}

std::vector<FetchPartitionResult> fetch(
    const Topic & topic,
    const std::vector<FetchPartitionRequest> & requests,
    size_t max_bytes,
    const QueryContextFactory & make_context)
{
    std::vector<FetchPartitionResult> results(requests.size());

    std::vector<Int32> partitions;
    for (const auto & request : requests)
        if (topic.hasPartition(request.partition))
            partitions.push_back(request.partition);

    const auto states = getPartitionStates(topic, partitions, make_context);

    std::map<String, PendingRead> pending;

    for (size_t i = 0; i < requests.size(); ++i)
    {
        const auto & request = requests[i];
        auto & result = results[i];
        result.partition = request.partition;

        if (!topic.hasPartition(request.partition))
        {
            result.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            continue;
        }

        const auto & state = states.at(request.partition);
        result.high_watermark = state.high_watermark;
        result.log_start_offset = state.log_start_offset;
        result.safe_block_number = state.safe_block_number;

        if (request.offset < 0 || request.offset > state.high_watermark)
        {
            result.error = ErrorCode::OFFSET_OUT_OF_RANGE;
            continue;
        }

        const Int64 start_offset = std::max(request.offset, state.log_start_offset);
        if (start_offset >= state.high_watermark || state.safe_block_number < 0)
            continue;

        const String partition_id = topic.partitionId(request.partition);
        if (pending.contains(partition_id))
            continue;

        PendingRead & read = pending[partition_id];
        read.result_index = i;
        read.partition_id = partition_id;
        read.start_offset = start_offset;
        read.high_watermark = state.high_watermark;
        read.safe_block_number = state.safe_block_number;
        read.max_bytes = std::max<Int32>(request.max_bytes, 1);
        read.bound = chooseBlockNumberBound(topic, partition_id, start_offset >> 32, state.safe_block_number, std::min(read.max_bytes, max_bytes));
    }

    if (pending.empty())
        return results;

    size_t total_bytes = 0;
    std::vector<PendingRead *> reads;
    for (auto & [_, read] : pending)
        reads.push_back(&read);

    bool complete = readPartitions(topic, reads, max_bytes, total_bytes, make_context);

    /// The estimated bound may be before the next record of a partition: read such partitions up to their end.
    if (complete)
    {
        reads.clear();
        for (auto & [_, read] : pending)
        {
            if (read.records.empty() && read.bound < read.safe_block_number)
            {
                read.bound = read.safe_block_number;
                reads.push_back(&read);
            }
        }
        if (!reads.empty())
            complete = readPartitions(topic, reads, max_bytes, total_bytes, make_context);
    }

    for (auto & [_, read] : pending)
    {
        FetchPartitionResult & result = results[read.result_index];
        appendBatches(result, read.records);

        /// There are no records between the requested offset and the high watermark (they were deleted):
        /// an empty batch moves the position of the consumer to the high watermark, as after a compaction in Kafka.
        if (complete && read.records.empty() && read.bound >= read.safe_block_number)
            writeEmptyRecordBatch(result.records, read.high_watermark - 1);
    }

    return results;
}


void produce(
    const Topic & topic,
    Int32 partition,
    const std::vector<RecordBatch> & batches,
    const QueryContextFactory & make_context)
{
    for (const auto & batch : batches)
    {
        if (batch.isTransactional() || batch.isControl())
            throw Error(ErrorCode::INVALID_RECORD, ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported");

        if (batch.records.empty())
            continue;

        auto context = make_context();
        Block block;

        /// The index of the record of every row: a message in a format may contain any number of rows.
        std::vector<size_t> row_records;

        if (topic.format.empty())
        {
            row_records.resize(batch.records.size());
            std::iota(row_records.begin(), row_records.end(), 0);
        }
        else
        {
            Block header;
            for (const auto & column : topic.value_columns)
                header.insert({column.type->createColumn(), column.type, column.name});

            EmptyReadBuffer empty_buf;
            auto parser_shared_resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());
            auto create_format = [&]
            {
                return FormatFactory::instance().getInput(
                    topic.format, empty_buf, header, context, DEFAULT_BLOCK_SIZE, std::nullopt, parser_shared_resources);
            };

            /// Every message is parsed separately, as in the `Kafka` table engine.
            StreamingFormatExecutor executor(header, std::move(create_format));
            for (size_t i = 0; i < batch.records.size(); ++i)
            {
                const auto & value = batch.records[i].value;
                if (!value)
                    throw Error(ErrorCode::INVALID_RECORD, ErrorCodes::INCORRECT_DATA,
                        "A record without a value cannot be written into the topic {} of the format {}", topic.name, topic.format);

                ReadBufferFromString buf(*value);
                size_t rows = executor.execute(buf);
                row_records.insert(row_records.end(), rows, i);
            }

            auto result_columns = executor.getResultColumns();
            for (size_t i = 0; i < header.columns(); ++i)
                block.insert({std::move(result_columns[i]), header.getByPosition(i).type, header.getByPosition(i).name});
        }

        const size_t num_rows = row_records.size();
        if (num_rows == 0)
            continue;

        if (!topic.partition_column.empty())
        {
            auto column = ColumnInt64::create(num_rows, partition);
            block.insert({std::move(column), std::make_shared<DataTypeInt64>(), topic.partition_column});
        }

        auto add_string_column = [&](const DataTypePtr & target_type, const String & name, auto && get)
        {
            if (!target_type)
                return;

            const bool nullable = target_type->isNullable();
            auto strings = ColumnString::create();
            auto null_map = ColumnUInt8::create();
            for (size_t record_index : row_records)
            {
                const std::optional<String> & value = get(batch.records[record_index]);
                strings->insertData(value ? value->data() : "", value ? value->size() : 0);
                null_map->insertValue(!value);
            }

            if (nullable)
                block.insert({ColumnNullable::create(std::move(strings), std::move(null_map)), makeNullable(std::make_shared<DataTypeString>()), name});
            else
                block.insert({std::move(strings), std::make_shared<DataTypeString>(), name});
        };

        add_string_column(topic.key_type, "key", [](const Record & record) -> const std::optional<String> & { return record.key; });
        add_string_column(topic.value_type, "value", [](const Record & record) -> const std::optional<String> & { return record.value; });

        if (topic.timestamp_type)
        {
            auto type = std::make_shared<DataTypeDateTime64>(3);
            auto column = type->createColumn();
            auto & data = assert_cast<ColumnDateTime64 &>(*column).getData();
            data.reserve(num_rows);
            for (size_t record_index : row_records)
                data.push_back(batch.records[record_index].timestamp);
            block.insert({std::move(column), type, "timestamp"});
        }

        if (topic.headers_type)
        {
            auto type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
            auto keys = ColumnString::create();
            auto values = ColumnString::create();
            auto offsets = ColumnArray::ColumnOffsets::create();
            UInt64 offset = 0;
            for (size_t record_index : row_records)
            {
                for (const auto & [header_key, header_value] : batch.records[record_index].headers)
                {
                    keys->insertData(header_key.data(), header_key.size());
                    values->insertData(header_value ? header_value->data() : "", header_value ? header_value->size() : 0);
                    ++offset;
                }
                offsets->getData().push_back(offset);
            }

            MutableColumns tuple_columns;
            tuple_columns.push_back(std::move(keys));
            tuple_columns.push_back(std::move(values));
            MutableColumnPtr tuple = ColumnTuple::create(std::move(tuple_columns));
            MutableColumnPtr offsets_column = std::move(offsets);
            MutableColumnPtr nested = ColumnArray::create(std::move(tuple), std::move(offsets_column));
            block.insert({ColumnMap::create(std::move(nested)), type, "headers"});
        }

        String query = fmt::format("INSERT INTO {} (", tableName(topic));
        for (size_t i = 0; i < block.columns(); ++i)
            query += (i ? ", " : "") + backQuote(block.getByPosition(i).name);
        query += ") FORMAT Native\n";

        {
            WriteBufferFromString out(query, AppendModeTag{});
            NativeWriter writer(out, 0, std::make_shared<const Block>(block.cloneEmpty()));
            writer.write(block);
            writer.flush();
            out.finalize();
        }

        if (batch.producer_id >= 0)
        {
            /// An idempotent producer retries a batch with the same sequence number.
            context->setSetting("insert_deduplicate", Field(true));
            context->setSetting("insert_deduplication_token", Field(fmt::format(
                "kafka:{}:{}:{}:{}", batch.producer_id, batch.producer_epoch, partition, batch.base_sequence)));
        }

        runQueryWithData(query, context);
    }
}


OffsetForTimestamp getOffsetForTimestamp(const Topic & topic, Int32 partition, Int64 timestamp, const QueryContextFactory & make_context)
{
    OffsetForTimestamp res;
    const auto states = getPartitionStates(topic, {partition}, make_context);
    const auto & state = states.at(partition);

    if (timestamp == -1)
    {
        res.offset = state.high_watermark;
        return res;
    }
    if (timestamp == -2)
    {
        res.offset = state.log_start_offset;
        return res;
    }

    if (!topic.timestamp_type || state.safe_block_number < 0)
        return res;

    String timestamp_expression = fmt::format("toUnixTimestamp64Milli(CAST({} AS DateTime64(3)))", backQuote("timestamp"));
    String condition;
    String order;
    if (timestamp == -3)
    {
        order = fmt::format("{} DESC, _block_number, _block_offset", timestamp_expression);
    }
    else
    {
        condition = fmt::format(" AND {} >= {}", timestamp_expression, timestamp);
        order = "_block_number, _block_offset";
    }

    runSelect(
        fmt::format(
            "SELECT _block_number, _block_offset, {} FROM {} WHERE _partition_id = {} AND _block_number <= {}{} ORDER BY {} LIMIT 1",
            timestamp_expression, tableName(topic), quoteString(topic.partitionId(partition)), state.safe_block_number, condition, order),
        make_context(),
        [&](const Block & block)
        {
            if (block.rows())
            {
                res.offset = makeOffset(block.getByPosition(0).column->getUInt(0), block.getByPosition(1).column->getUInt(0));
                res.timestamp = block.getByPosition(2).column->getInt(0);
            }
            return true;
        });

    return res;
}


void createTopic(
    const String & name,
    Int32 num_partitions,
    const std::vector<std::pair<String, std::optional<String>>> & configs,
    bool validate_only,
    const BrokerSettings & settings,
    const QueryContextFactory & make_context)
{
    if (num_partitions == -1)
        num_partitions = 1;
    if (num_partitions <= 0)
        throw Error(ErrorCode::INVALID_PARTITIONS, ErrorCodes::BAD_ARGUMENTS, "The number of partitions must be positive");

    StorageID table_id(settings.database, name);
    if (auto dot = name.find('.'); dot != String::npos && dot > 0 && dot + 1 < name.size()
        && DatabaseCatalog::instance().isDatabaseExist(name.substr(0, dot)))
        table_id = StorageID(name.substr(0, dot), name.substr(dot + 1));

    String ttl;
    for (const auto & [config_name, config_value] : configs)
    {
        if (config_name == "retention.ms" && config_value)
        {
            Int64 retention_ms = 0;
            if (!tryParse(retention_ms, *config_value))
                throw Error(ErrorCode::INVALID_CONFIG, ErrorCodes::BAD_ARGUMENTS, "Invalid value of `retention.ms`: {}", *config_value);
            if (retention_ms > 0)
                ttl = fmt::format(" TTL toDateTime(timestamp) + toIntervalSecond({})", (retention_ms + 999) / 1000);
        }
    }

    String query = fmt::format(
        "CREATE TABLE {} (partition UInt32, key Nullable(String), value Nullable(String), timestamp DateTime64(3), headers Map(String, String), "
        "CONSTRAINT partition_count CHECK partition < {}) "
        "ENGINE = {} PARTITION BY partition ORDER BY tuple(){} "
        "SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, "
        "add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1, "
        "part_minmax_index_columns = 'with_block_number_offset'",
        table_id.getFullTableName(), num_partitions, settings.topic_engine, ttl);

    if (validate_only)
        return;

    runQuery(query, make_context());
}

void deleteTopic(const StorageID & table_id, const QueryContextFactory & make_context)
{
    runQuery(fmt::format("DROP TABLE {} SYNC", table_id.getFullTableName()), make_context());
}


void runSelect(const String & query, ContextMutablePtr context, const std::function<bool(const Block &)> & on_block)
{
    QueryScope query_scope = QueryScope::create(context);
    auto [ast, io] = executeQuery(query, context);

    try
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() && !on_block(block))
            {
                executor.cancel();
                break;
            }
        }
        io.onFinish();
    }
    catch (...)
    {
        io.onException();
        throw;
    }
}

void runQueryWithData(const String & query_with_data, ContextMutablePtr context)
{
    QueryScope query_scope = QueryScope::create(context);
    ReadBufferFromString in(query_with_data);
    NullWriteBuffer out;
    executeQuery(in, out, context, {});
    out.finalize();
}

void runQuery(const String & query, ContextMutablePtr context)
{
    QueryScope query_scope = QueryScope::create(context);
    auto [ast, io] = executeQuery(query, context);

    try
    {
        if (io.pipeline.initialized())
        {
            if (io.pipeline.pulling())
            {
                PullingPipelineExecutor executor(io.pipeline);
                Block block;
                while (executor.pull(block))
                {
                }
            }
            else
            {
                executeTrivialBlockIO(io, context);
                return;
            }
        }
        io.onFinish();
    }
    catch (...)
    {
        io.onException();
        throw;
    }
}

}

}
