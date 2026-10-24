#include "config.h"

#if USE_AVRO

#include <algorithm>
#include <compare>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <Poco/String.h>

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

static std::strong_ordering operator<=>(const PartitionSpecsEntry & lhs, const PartitionSpecsEntry & rhs)
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
    return std::tie(*lhs->common_partition_specification, lhs->normalized_partition_key_value, lhs->sequence_number)
        <=> std::tie(*rhs->common_partition_specification, rhs->normalized_partition_key_value, rhs->sequence_number);
}

std::span<const ProcessedManifestFileEntryPtr> defineDeletesSpan(
    ProcessedManifestFileEntryPtr data_object_,
    const std::vector<ProcessedManifestFileEntryPtr> & deletes_objects,
    bool is_equality_delete,
    LoggerPtr logger)
{
    if (deletes_objects.empty())
    {
        return {};
    }
    /// Objects in deletes_objects are sorted by common_partition_specification, partition_key_value and added_sequence_number.
    /// It is done to have an invariant that position deletes objects which corresponds
    /// to the data object form a subsegment in a deletes_objects vector.
    /// We need to take all position deletes objects which has the same partition schema and value and has added_sequence_number
    /// greater than or equal to the data object added_sequence_number (https://iceberg.apache.org/spec/#scan-planning)
    /// ManifestFileEntry has comparator by default which helps to do that.
    auto beg_it = is_equality_delete ?
        std::upper_bound(deletes_objects.begin(), deletes_objects.end(), data_object_)
        : std::lower_bound(deletes_objects.begin(), deletes_objects.end(), data_object_);
    auto end_it = std::upper_bound(
        deletes_objects.begin(),
        deletes_objects.end(),
        data_object_,
        [](const ProcessedManifestFileEntryPtr & lhs, const ProcessedManifestFileEntryPtr & rhs)
        {
            return std::tie(*lhs->common_partition_specification, lhs->normalized_partition_key_value)
                < std::tie(*rhs->common_partition_specification, rhs->normalized_partition_key_value);
        });
    if (beg_it - deletes_objects.begin() > end_it - deletes_objects.begin())
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "{} deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            is_equality_delete ? "Equality" : "Position",
            beg_it - deletes_objects.begin(),
            end_it - deletes_objects.begin(),
            deletes_objects.size());
    }
    if (beg_it != end_it)
    {
        auto previous_it = std::prev(end_it);
        chassert(*beg_it);
        chassert(*previous_it);
        LOG_DEBUG(
            logger,
            "Preliminary check got {} {} delete elements for data file {}, taken data file object info: {}, first taken delete object info is "
            "{}, last taken "
            "delete object info is {}",
            std::distance(beg_it, end_it),
            is_equality_delete ? "equality" : "position",
            data_object_->parsed_entry->file_path_key,
            data_object_->dumpDeletesMatchingInfo(),
            (*beg_it)->dumpDeletesMatchingInfo(),
            (*previous_it)->dumpDeletesMatchingInfo());
    }
    else
    {
        LOG_DEBUG(
            logger,
            "No {} delete elements for data file {}, taken data file object info: {}",
            is_equality_delete ? "equality" : "position",
            data_object_->parsed_entry->file_path_key,
            data_object_->dumpDeletesMatchingInfo());
    }
    return {beg_it, end_it};
}

static String dumpPartitionSpecification(const PartitionSpecification & partition_specification)
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

static String dumpPartitionKeyValue(const DB::Row & partition_key_value)
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


bool ParsedManifestFileEntry::isDeletionVector() const
{
    return content_type == FileContentType::POSITION_DELETE && Poco::toLower(file_format) == "puffin";
}

String ProcessedManifestFileEntry::dumpDeletesMatchingInfo() const
{
    return fmt::format(
        "Partition specification: {}, partition key value: {}, added sequence number: {}",
        dumpPartitionSpecification(*common_partition_specification),
        dumpPartitionKeyValue(normalized_partition_key_value),
        sequence_number);
}
}


#endif
