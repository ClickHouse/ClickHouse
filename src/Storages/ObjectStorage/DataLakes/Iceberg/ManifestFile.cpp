#include "config.h"

#if USE_AVRO

#include <compare>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <Common/logger_useful.h>
#include <fmt/format.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::Iceberg
{

String FileContentTypeToString(FileContentType type)
{
    switch (type)
    {
        case FileContentType::DATA:
            return "data";
        case FileContentType::POSITION_DELETE:
            return "position_deletes";
        case FileContentType::EQUALITY_DELETE:
            return "equality_deletes";
    }
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported content type: {}", static_cast<int>(type));
}

std::strong_ordering operator<=>(const PartitionSpecsEntry & lhs, const PartitionSpecsEntry & rhs)
{
    return std::tie(lhs.source_id, lhs.transform_name, lhs.partition_name)
        <=> std::tie(rhs.source_id, rhs.transform_name, rhs.partition_name);
}

template <typename A>
bool less(const std::vector<A> & lhs, const std::vector<A> & rhs)
{
    if (lhs.size() != rhs.size())
        return lhs.size() < rhs.size();
    return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const A & a, const A & b) { return a < b; });
}

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs)
{
    return less(lhs, rhs);
}

bool operator<(const DB::Row & lhs, const DB::Row & rhs)
{
    return less(lhs, rhs);
}

std::weak_ordering operator<=>(const ProcessedManifestFileEntryPtr & lhs, const ProcessedManifestFileEntryPtr & rhs)
{
    return std::tie(*lhs->common_partition_specification, lhs->parsed_entry->partition_key_value, lhs->sequence_number)
        <=> std::tie(*rhs->common_partition_specification, rhs->parsed_entry->partition_key_value, rhs->sequence_number);
}

String dumpPartitionSpecification(const PartitionSpecification & partition_specification)
{
    if (partition_specification.empty())
        return "[empty]";
    else
    {
        String answer{"["};
        for (size_t i = 0; i < partition_specification.size(); ++i)
        {
            const auto & entry = partition_specification[i];
            answer += fmt::format(
                "(source id: {}, transform name: {}, partition name: {})", entry.source_id, entry.transform_name, entry.partition_name);
            if (i != partition_specification.size() - 1)
                answer += ", ";
        }
        answer += ']';
        return answer;
    }
}

String dumpPartitionKeyValue(const DB::Row & partition_key_value)
{
    if (partition_key_value.empty())
        return "[empty]";
    else
    {
        String answer{"["};
        for (size_t i = 0; i < partition_key_value.size(); ++i)
        {
            const auto & entry = partition_key_value[i];
            answer += entry.dump();
            if (i != partition_key_value.size() - 1)
                answer += ", ";
        }
        answer += ']';
        return answer;
    }
}


String ProcessedManifestFileEntry::dumpDeletesMatchingInfo() const
{
    return fmt::format(
        "Partition specification: {}, partition key value: {}, added sequence number: {}",
        dumpPartitionSpecification(*common_partition_specification),
        dumpPartitionKeyValue(parsed_entry->partition_key_value),
        sequence_number);
}
}


#endif
