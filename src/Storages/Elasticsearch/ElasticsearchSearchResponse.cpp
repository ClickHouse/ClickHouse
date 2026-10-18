#include <Storages/Elasticsearch/ElasticsearchSearchResponse.h>

#include <Columns/IColumn.h>
#include <Formats/JSONUtils.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

Poco::JSON::Object::Ptr parseElasticsearchJSONObject(const String & data)
{
    try
    {
        Poco::JSON::Parser parser;
        auto object = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
        if (!object)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse Elasticsearch response: not a JSON object");
        return object;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse Elasticsearch response: {}", e.displayText());
    }
}

void validateElasticsearchShards(const Poco::JSON::Object::Ptr & shards)
{
    if (!shards)
        return;

    const auto failed = shards->optValue<Int64>("failed", -1);
    const auto total = shards->optValue<Int64>("total", -1);
    if (failed < 0 || total < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no '_shards' counters");
    if (failed == 0)
        return;

    String reason;
    if (auto failures = shards->getArray("failures"); failures && failures->size() > 0)
    {
        auto failure = failures->getObject(0);
        if (auto inner = failure->getObject("reason"))
            reason = inner->optValue<String>("reason", "");
    }

    throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
        "Elasticsearch operation failed on {} of {} shards, first failure: {}", failed, total, reason);
}

ElasticsearchSearchResponse::ElasticsearchSearchResponse(
    std::unique_ptr<ReadBuffer> in_, const ElasticsearchHitColumns & hit_columns_, const FormatSettings & format_settings_)
    : in(std::move(in_))
    , hit_columns(hit_columns_)
    , format_settings(format_settings_)
{
    readHeader();
}

void ElasticsearchSearchResponse::readRootField(const String & name)
{
    skipWhitespaceIfAny(*in);

    if (name == "pit_id")
    {
        readJSONString(pit_id, *in, format_settings.json);
    }
    else if (name == "timed_out")
    {
        bool timed_out = false;
        readBoolTextWord(timed_out, *in);
        if (timed_out)
            throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "Elasticsearch request timed out");
    }
    else if (name == "_shards")
    {
        String shards;
        readJSONObjectPossiblyInvalid(shards, *in);
        validateElasticsearchShards(parseElasticsearchJSONObject(shards));
    }
    else
    {
        skipJSONField(*in, name, format_settings.json);
    }
}

void ElasticsearchSearchResponse::readHeader()
{
    JSONUtils::skipObjectStart(*in);
    for (size_t i = 0;; ++i)
    {
        if (JSONUtils::checkAndSkipObjectEnd(*in))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits' object");
        if (i > 0)
            JSONUtils::skipComma(*in);

        auto name = JSONUtils::readFieldName(*in, format_settings.json);
        if (name == "hits")
            break;
        readRootField(name);
    }

    JSONUtils::skipObjectStart(*in);
    for (size_t i = 0;; ++i)
    {
        if (JSONUtils::checkAndSkipObjectEnd(*in))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'hits.hits' array");
        if (i > 0)
            JSONUtils::skipComma(*in);

        auto name = JSONUtils::readFieldName(*in, format_settings.json);
        if (name == "hits")
            break;
        skipWhitespaceIfAny(*in);
        skipJSONField(*in, name, format_settings.json);
    }

    JSONUtils::skipArrayStart(*in);
}

bool ElasticsearchSearchResponse::readHit(MutableColumns & columns)
{
    if (finished)
        return false;

    if (JSONUtils::checkAndSkipArrayEnd(*in))
    {
        readTail();
        finished = true;
        return false;
    }
    if (hits_read > 0)
        JSONUtils::skipComma(*in);

    index_name.clear();
    document_id.clear();
    last_sort.clear();
    bool has_source = false;

    JSONUtils::skipObjectStart(*in);
    for (size_t i = 0; !JSONUtils::checkAndSkipObjectEnd(*in); ++i)
    {
        if (i > 0)
            JSONUtils::skipComma(*in);

        auto name = JSONUtils::readFieldName(*in, format_settings.json);
        skipWhitespaceIfAny(*in);

        if (name == "_index" && (hit_columns.index_pos || hit_columns.source_pos))
        {
            readJSONString(index_name, *in, format_settings.json);
        }
        else if (name == "_id" && hit_columns.id_pos)
        {
            readJSONString(document_id, *in, format_settings.json);
        }
        else if (name == "_source" && hit_columns.source_pos)
        {
            hit_columns.source_serialization->deserializeTextJSON(*columns[*hit_columns.source_pos], *in, format_settings);
            has_source = true;
        }
        else if (name == "sort")
        {
            readJSONArrayInto(last_sort, *in);
        }
        else
        {
            skipJSONField(*in, name, format_settings.json);
        }
    }

    if (hit_columns.index_pos)
    {
        if (index_name.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Hit #{} has no '_index'", hits_read);
        columns[*hit_columns.index_pos]->insertData(index_name.data(), index_name.size());
    }
    if (hit_columns.id_pos)
    {
        if (document_id.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Hit #{} has no '_id'", hits_read);
        columns[*hit_columns.id_pos]->insertData(document_id.data(), document_id.size());
    }
    if (hit_columns.source_pos && !has_source)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Hit #{} from index '{}' has no '_source'. You can read only the virtual columns '_id' and '_index'",
            hits_read, index_name);

    ++hits_read;
    return true;
}

void ElasticsearchSearchResponse::readTail()
{
    JSONUtils::skipTheRestOfObject(*in, format_settings.json);

    while (!JSONUtils::checkAndSkipObjectEnd(*in))
    {
        JSONUtils::skipComma(*in);
        auto name = JSONUtils::readFieldName(*in, format_settings.json);
        readRootField(name);
    }

    skipWhitespaceIfAny(*in);
    if (!in->eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the end of the Elasticsearch response");

    if (pit_id.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Elasticsearch response has no 'pit_id'");
}

}
