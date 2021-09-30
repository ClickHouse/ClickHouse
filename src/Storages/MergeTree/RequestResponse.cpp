#include <Storages/MergeTree/RequestResponse.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>


namespace DB
{


void PartitionReadRequest::serialize(WriteBuffer & out) const
{
    writeStringBinary(partition_id, out);
    writeStringBinary(part_name, out);
    writeStringBinary(projection_name, out);

    writeVarInt(block_range.begin, out);
    writeVarInt(block_range.end, out);

    writeDequeBinary(mark_ranges, out);
}


void PartitionReadRequest::describe(WriteBuffer & out) const
{
    String result;
    result += fmt::format("partition_id: {} \n", partition_id);
    result += fmt::format("part_name: {} \n", part_name);
    result += fmt::format("projection_name: {} \n", projection_name);
    result += fmt::format("block_range: ({}, {}) \n", block_range.begin, block_range.end);
    result += "mark_ranges: ";
    for (const auto & range : mark_ranges)
        result += fmt::format("({}, {}) ", range.begin, range.end);
    result += '\n';
    out.write(result.c_str(), result.size());
}


void PartitionReadRequest::serializeToJSON(WriteBuffer & out) const
{
    Poco::JSON::Object result_json;

    result_json.set("partition_id", partition_id);
    result_json.set("part_name", part_name);
    result_json.set("projeciton_name", projection_name); // FIXME, typo

    result_json.set("begin_part", block_range.begin);
    result_json.set("end_part", block_range.end);

    Poco::JSON::Array open;
    Poco::JSON::Array close;

    for (const auto & range : mark_ranges)
    {
        open.add(range.begin);
        close.add(range.end);
    }

    result_json.set("open_marks", open);
    result_json.set("close_marks", close);

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(result_json, oss);
    auto result = oss.str();

    out.write(result.c_str(), result.size());
}


void PartitionReadRequest::deserializeFromJSON(String line)
{
    Poco::JSON::Parser parser;
    auto state = parser.parse(line).extract<Poco::JSON::Object::Ptr>();

    partition_id = state->get("partition_id").toString();
    part_name = state->get("part_name").toString();
    projection_name = state->get("projeciton_name").toString(); // FIXME: typo

    block_range.begin = state->get("begin_part").convert<Int64>();
    block_range.end = state->get("end_part").convert<Int64>();

    auto open = state->getArray("open_marks");
    auto close = state->getArray("close_marks");

    assert(open->size() == close->size());
    for (size_t i = 0; i < open->size(); ++i)
        mark_ranges.emplace_back(open->getElement<UInt64>(i), close->getElement<UInt64>(i));
}


void PartitionReadRequest::deserialize(ReadBuffer & in)
{
    readStringBinary(partition_id, in);
    readStringBinary(part_name, in);
    readStringBinary(projection_name, in);

    readVarInt(block_range.begin, in);
    readVarInt(block_range.end, in);

    readDequeBinary(mark_ranges, in);
}


void PartitionReadResponce::serialize(WriteBuffer & out) const
{
    writeVarUInt(static_cast<UInt64>(denied), out);
    writeDequeBinary(mark_ranges, out);
}


void PartitionReadResponce::deserialize(ReadBuffer & in)
{
    UInt64 value;
    readVarUInt(value, in);
    denied = static_cast<bool>(value);
    readDequeBinary(mark_ranges, in);
}

}
