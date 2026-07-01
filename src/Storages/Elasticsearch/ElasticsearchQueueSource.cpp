#include <Storages/Elasticsearch/ElasticsearchQueueSource.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Stringifier.h>

#include <optional>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

namespace
{
String serializeJSON(const Poco::Dynamic::Var & value)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(value, oss);
    return oss.str();
}

std::optional<Poco::Dynamic::Var> getFieldValue(const StorageElasticsearchQueue::Row & row, const String & column_name)
{
    if (column_name == "_id" || column_name == "_index" || column_name == "_score" || column_name == "_source")
    {
        if (row.hit->has(column_name))
            return row.hit->get(column_name);
        return std::nullopt;
    }

    if (row.source && row.source->has(column_name))
        return row.source->get(column_name);

    return std::nullopt;
}

void insertValue(IColumn & column, const DataTypePtr & type, const Poco::Dynamic::Var & value, const String & column_name)
{
    if (value.isEmpty())
    {
        column.insertDefault();
        return;
    }

    if (WhichDataType(type).isString())
    {
        const String raw_value = value.isString() ? value.extract<String>() : serializeJSON(value);
        column.insertData(raw_value.data(), raw_value.size());
        return;
    }

    const String json_value = serializeJSON(value);
    ReadBufferFromString input(json_value);
    if (!type->getDefaultSerialization()->tryDeserializeTextJSON(column, input, FormatSettings{}))
    {
        throw Exception(
            ErrorCodes::CANNOT_PARSE_TEXT,
            "Cannot parse Elasticsearch field '{}' value '{}' as ClickHouse type {}",
            column_name,
            json_value,
            type->getName());
    }
}
}

ElasticsearchQueueSource::ElasticsearchQueueSource(
    StorageElasticsearchQueue & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    size_t max_block_size_,
    bool commit_in_suffix_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , commit_in_suffix(commit_in_suffix_)
{
}

ElasticsearchQueueSource::~ElasticsearchQueueSource()
{
    releaseKeeperLock();
}

Chunk ElasticsearchQueueSource::generate()
{
    if (is_finished)
    {
        if (commit_in_suffix)
            commit();
        return {};
    }

    is_finished = true;

    auto batch = storage.pollBatch(max_block_size);
    checkpoint = std::move(batch.checkpoint);
    checkpoint_zookeeper = std::move(batch.checkpoint_zookeeper);
    checkpoint_lock = std::move(batch.checkpoint_lock);

    if (batch.rows.empty())
    {
        stalled = true;
        if (commit_in_suffix)
            commit();
        return {};
    }

    const auto & header = getPort().getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    const auto & columns_with_type = header.getColumnsWithTypeAndName();

    for (const auto & row : batch.rows)
    {
        for (size_t i = 0; i < columns_with_type.size(); ++i)
        {
            const auto & column_name = columns_with_type[i].name;
            auto value = getFieldValue(row, column_name);
            if (!value)
                columns[i]->insertDefault();
            else
                insertValue(*columns[i], columns_with_type[i].type, *value, column_name);
        }
    }

    return Chunk(std::move(columns), batch.rows.size());
}

void ElasticsearchQueueSource::commit()
{
    if (committed || checkpoint.empty())
        return;

    storage.commit(checkpoint, checkpoint_lock, checkpoint_zookeeper);
    releaseKeeperLock();
    committed = true;
}

void ElasticsearchQueueSource::releaseKeeperLock()
{
    if (!checkpoint_lock && !checkpoint_zookeeper)
        return;

    auto component_guard = Coordination::setCurrentComponent("ElasticsearchQueueSource::releaseKeeperLock");
    checkpoint_lock.reset();
    checkpoint_zookeeper.reset();
}

}
