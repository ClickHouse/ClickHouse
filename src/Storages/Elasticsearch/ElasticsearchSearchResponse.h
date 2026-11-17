#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <Poco/JSON/Object.h>

#include <memory>
#include <optional>

namespace DB
{

struct ElasticsearchHitColumns
{
    std::optional<size_t> index_pos;
    std::optional<size_t> id_pos;
    std::optional<size_t> source_pos;
    SerializationPtr source_serialization;
};

Poco::JSON::Object::Ptr parseElasticsearchJSONObject(const String & data);

void validateElasticsearchShards(const Poco::JSON::Object::Ptr & shards);

class ElasticsearchSearchResponse
{
public:
    ElasticsearchSearchResponse(std::unique_ptr<ReadBuffer> in_, const ElasticsearchHitColumns & hit_columns_, const FormatSettings & format_settings_);

    bool readHit(MutableColumns & columns);

    size_t hitsRead() const { return hits_read; }
    const String & pitId() const { return pit_id; }
    const String & lastSort() const { return last_sort; }

private:
    void readHeader();
    void readTail();
    void readRootField(const String & name);

    std::unique_ptr<ReadBuffer> in;
    const ElasticsearchHitColumns & hit_columns;
    const FormatSettings & format_settings;

    size_t hits_read = 0;
    bool finished = false;
    String pit_id;
    String last_sort;
};

}
