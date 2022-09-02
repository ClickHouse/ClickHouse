#include <Common/ZooKeeper/Types.h>
#include "Access/IAccessEntity.h"

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}

enum FormatVersion : UInt8
{
    FORMAT_WITH_CREATE_TIME = 2,
    FORMAT_WITH_BLOCK_ID = 3,
    FORMAT_WITH_DEDUPLICATE = 4,
    FORMAT_WITH_UUID = 5,
    FORMAT_WITH_DEDUPLICATE_BY_COLUMNS = 6,
    FORMAT_WITH_LOG_ENTRY_ID = 7,

    FORMAT_LAST
};


void ReplicatedMergeTreeLogEntryData::writeText(WriteBuffer & out) const
{
    UInt8 format_version = FORMAT_WITH_DEDUPLICATE;

    if (!deduplicate_by_columns.empty())
        format_version = std::max<UInt8>(format_version, FORMAT_WITH_DEDUPLICATE_BY_COLUMNS);

    /// Conditionally bump format_version only when uuid has been assigned.
    /// If some other feature requires bumping format_version to >= 5 then this code becomes no-op.
    if (new_part_uuid != UUIDHelpers::Nil)
        format_version = std::max<UInt8>(format_version, FORMAT_WITH_UUID);

    if (!log_entry_id.empty())
        format_version = std::max<UInt8>(format_version, FORMAT_WITH_LOG_ENTRY_ID);

    out << "format version: " << format_version << "\n"
        << "create_time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "source replica: " << source_replica << '\n'
        << "block_id: " << escape << block_id << '\n';

    if (format_version >= FORMAT_WITH_LOG_ENTRY_ID)
        out << "log_entry_id: " << escape << log_entry_id << '\n';

    switch (type)
    {
        case GET_PART:
            out << "get\n" << new_part_name;
            break;

        case CLONE_PART_FROM_SHARD:
            out << "clone_part_from_shard\n"
                << new_part_name << "\n"
                << "source_shard: " << source_shard;
            break;

        case ATTACH_PART:
            out << "attach\n" << new_part_name << "\n"
                << "part_checksum: " << part_checksum;
            break;

        case MERGE_PARTS:
            out << "merge\n";
            for (const String & s : source_parts)
                out << s << '\n';
            out << "into\n" << new_part_name;
            out << "\ndeduplicate: " << deduplicate;

            if (merge_type != MergeType::REGULAR)
                out <<"\nmerge_type: " << static_cast<UInt64>(merge_type);

            if (new_part_uuid != UUIDHelpers::Nil)
                out << "\ninto_uuid: " << new_part_uuid;

            if (!deduplicate_by_columns.empty())
            {
                out << "\ndeduplicate_by_columns: ";
                for (size_t i = 0; i < deduplicate_by_columns.size(); ++i)
                {
                    out << quote << deduplicate_by_columns[i];
                    if (i != deduplicate_by_columns.size() - 1)
                        out << ",";
                }
            }

            break;

        case DROP_RANGE:
            if (detach)
                out << "detach\n";
            else
                out << "drop\n";
            out << new_part_name;
            break;

        /// NOTE: Deprecated.
        case CLEAR_COLUMN:
            out << "clear_column\n"
                << escape << column_name
                << "\nfrom\n"
                << new_part_name;
            break;

        /// NOTE: Deprecated.
        case CLEAR_INDEX:
            out << "clear_index\n"
                << escape << index_name
                << "\nfrom\n"
                << new_part_name;
            break;

        case REPLACE_RANGE:
            out << typeToString(REPLACE_RANGE) << "\n";
            replace_range_entry->writeText(out);
            break;

        case MUTATE_PART:
            out << "mutate\n"
                << source_parts.at(0) << "\n"
                << "to\n"
                << new_part_name;

            if (new_part_uuid != UUIDHelpers::Nil)
                out << "\nto_uuid\n"
                    << new_part_uuid;

            if (isAlterMutation())
                out << "\nalter_version\n" << alter_version;
            break;

        case ALTER_METADATA: /// Just make local /metadata and /columns consistent with global
            out << "alter\n";
            out << "alter_version\n";
            out << alter_version<< "\n";
            out << "have_mutation\n";
            out << have_mutation << "\n";
            out << "columns_str_size:\n";
            out << columns_str.size() << "\n";
            out << columns_str << "\n";
            out << "metadata_str_size:\n";
            out << metadata_str.size() << "\n";
            out << metadata_str;
            break;

        case SYNC_PINNED_PART_UUIDS:
            out << "sync_pinned_part_uuids\n";
            break;

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown log entry type: {}", static_cast<int>(type));
    }

    out << '\n';

    if (new_part_type != MergeTreeDataPartType::WIDE && new_part_type != MergeTreeDataPartType::UNKNOWN)
        out << "part_type: " << new_part_type.toString() << "\n";

    if (quorum)
        out << "quorum: " << quorum << '\n';
}

void ReplicatedMergeTreeLogEntryData::readText(ReadBuffer & in)
{
    UInt8 format_version = 0;
    String type_str;

    in >> "format version: " >> format_version >> "\n";

    if (format_version < 1 || format_version >= FORMAT_LAST)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown ReplicatedMergeTreeLogEntry format version: {}",
                DB::toString(format_version));

    if (format_version >= FORMAT_WITH_CREATE_TIME)
    {
        LocalDateTime create_time_dt;
        in >> "create_time: " >> create_time_dt >> "\n";
        create_time = DateLUT::instance().makeDateTime(
            create_time_dt.year(), create_time_dt.month(), create_time_dt.day(),
            create_time_dt.hour(), create_time_dt.minute(), create_time_dt.second());
    }

    in >> "source replica: " >> source_replica >> "\n";

    if (format_version >= FORMAT_WITH_BLOCK_ID)
    {
        in >> "block_id: " >> escape >> block_id >> "\n";
    }

    if (format_version >= FORMAT_WITH_LOG_ENTRY_ID)
        in >> "log_entry_id: " >> escape >> log_entry_id >> "\n";

    in >> type_str >> "\n";

    bool trailing_newline_found = false;

    if (type_str == "get")
    {
        type = GET_PART;
        in >> new_part_name;
    }
    else if (type_str == "attach")
    {
        type = ATTACH_PART;
        in >> new_part_name >> "\npart_checksum: " >> part_checksum;
    }
    else if (type_str == "merge")
    {
        type = MERGE_PARTS;
        while (true)
        {
            String s;
            in >> s >> "\n";
            if (s == "into")
                break;
            source_parts.push_back(s);
        }
        in >> new_part_name;

        if (format_version >= FORMAT_WITH_DEDUPLICATE)
        {
            in >> "\ndeduplicate: " >> deduplicate;

            /// Trying to be more backward compatible
            while (!trailing_newline_found)
            {
                in >> "\n";

                if (checkString("merge_type: ", in))
                {
                    UInt64 value;
                    in >> value;
                    merge_type = checkAndGetMergeType(value);
                }
                else if (checkString("into_uuid: ", in))
                    in >> new_part_uuid;
                else if (checkString("deduplicate_by_columns: ", in))
                {
                    Strings new_deduplicate_by_columns;
                    for (;;)
                    {
                        String tmp_column_name;
                        in >> quote >> tmp_column_name;
                        new_deduplicate_by_columns.emplace_back(std::move(tmp_column_name));
                        if (!checkString(",", in))
                            break;
                    }

                    deduplicate_by_columns = std::move(new_deduplicate_by_columns);
                }
                else
                    trailing_newline_found = true;
            }
        }
    }
    else if (type_str == "drop" || type_str == "detach")
    {
        type = DROP_RANGE;
        detach = type_str == "detach";
        in >> new_part_name;
    }
    else if (type_str == "clear_column") /// NOTE: Deprecated.
    {
        type = CLEAR_COLUMN;
        in >> escape >> column_name >> "\nfrom\n" >> new_part_name;
    }
    else if (type_str == "clear_index") /// NOTE: Deprecated.
    {
        type = CLEAR_INDEX;
        in >> escape >> index_name >> "\nfrom\n" >> new_part_name;
    }
    else if (type_str == typeToString(REPLACE_RANGE))
    {
        type = REPLACE_RANGE;
        replace_range_entry = std::make_shared<ReplaceRangeEntry>();
        replace_range_entry->readText(in);
    }
    else if (type_str == "mutate")
    {
        type = MUTATE_PART;
        String source_part;
        in >> source_part >> "\n"
           >> "to\n"
           >> new_part_name;
        source_parts.push_back(source_part);

        while (!trailing_newline_found)
        {
            in >> "\n";

            if (checkString("alter_version\n", in))
                in >> alter_version;
            else if (checkString("to_uuid\n", in))
                in >> new_part_uuid;
            else
                trailing_newline_found = true;
        }
    }
    else if (type_str == "alter")
    {
        type = ALTER_METADATA;
        in >> "alter_version\n";
        in >> alter_version;
        in >> "\nhave_mutation\n";
        in >> have_mutation;
        in >> "\ncolumns_str_size:\n";
        size_t columns_size;
        in >> columns_size >> "\n";
        columns_str.resize(columns_size);
        in.readStrict(&columns_str[0], columns_size);
        in >> "\nmetadata_str_size:\n";
        size_t metadata_size;
        in >> metadata_size >> "\n";
        metadata_str.resize(metadata_size);
        in.readStrict(&metadata_str[0], metadata_size);
    }
    else if (type_str == "sync_pinned_part_uuids")
    {
        type = SYNC_PINNED_PART_UUIDS;
    }
    else if (type_str == "clone_part_from_shard")
    {
        type = CLONE_PART_FROM_SHARD;
        in >> new_part_name;
        in >> "\nsource_shard: " >> source_shard;
    }

    if (!trailing_newline_found)
        in >> "\n";

    if (checkString("part_type: ", in))
    {
        String part_type_str;
        in >> type_str;
        new_part_type.fromString(type_str);
        in >> "\n";
    }
    else
        new_part_type = MergeTreeDataPartType::WIDE;

    /// Optional field.
    if (!in.eof())
        in >> "quorum: " >> quorum >> "\n";
}

void ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry::writeText(WriteBuffer & out) const
{
    out << "drop_range_name: " << drop_range_part_name << "\n";
    out << "from_database: " << escape << from_database << "\n";
    out << "from_table: " << escape << from_table << "\n";

    out << "source_parts: ";
    writeQuoted(src_part_names, out);
    out << "\n";

    out << "new_parts: ";
    writeQuoted(new_part_names, out);
    out << "\n";

    out << "part_checksums: ";
    writeQuoted(part_names_checksums, out);
    out << "\n";

    out << "columns_version: " << columns_version;
}

void ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry::readText(ReadBuffer & in)
{
    in >> "drop_range_name: " >> drop_range_part_name >> "\n";
    in >> "from_database: " >> escape >> from_database >> "\n";
    in >> "from_table: " >> escape >> from_table >> "\n";

    in >> "source_parts: ";
    readQuoted(src_part_names, in);
    in >> "\n";

    in >> "new_parts: ";
    readQuoted(new_part_names, in);
    in >> "\n";

    in >> "part_checksums: ";
    readQuoted(part_names_checksums, in);
    in >> "\n";

    in >> "columns_version: " >> columns_version;
}

bool ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry::isMovePartitionOrAttachFrom(const MergeTreePartInfo & drop_range_info)
{
    assert(drop_range_info.getBlocksCount() != 0);
    return drop_range_info.getBlocksCount() == 1;
}

String ReplicatedMergeTreeLogEntryData::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

ReplicatedMergeTreeLogEntry::Ptr ReplicatedMergeTreeLogEntry::parse(const String & s, const Coordination::Stat & stat)
{
    ReadBufferFromString in(s);
    Ptr res = std::make_shared<ReplicatedMergeTreeLogEntry>();
    res->readText(in);
    assertEOF(in);

    if (!res->create_time)
        res->create_time = stat.ctime / 1000;

    return res;
}

std::optional<String> ReplicatedMergeTreeLogEntryData::getDropRange(MergeTreeDataFormatVersion format_version) const
{
    if (type == DROP_RANGE)
        return new_part_name;

    if (type == REPLACE_RANGE)
    {
        auto drop_range_info = MergeTreePartInfo::fromPartName(replace_range_entry->drop_range_part_name, format_version);
        if (!ReplaceRangeEntry::isMovePartitionOrAttachFrom(drop_range_info))
        {
            /// It's REPLACE, not MOVE or ATTACH, so drop range is real
            return replace_range_entry->drop_range_part_name;
        }
    }

    return {};
}

bool ReplicatedMergeTreeLogEntryData::isDropPart(MergeTreeDataFormatVersion format_version) const
{
    if (type == DROP_RANGE)
    {
        auto drop_range_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);
        return !drop_range_info.isFakeDropRangePart();
    }
    return false;
}

Strings ReplicatedMergeTreeLogEntryData::getVirtualPartNames(MergeTreeDataFormatVersion format_version) const
{
    /// Doesn't produce any part
    if (type == ALTER_METADATA)
        return {};

    /// DROP_RANGE does not add a real part, but we must disable merges in that range
    if (type == DROP_RANGE)
    {
        auto drop_range_part_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);

        /// It's DROP PART and we don't want to add it into virtual parts
        /// because it can lead to intersecting parts on stale replicas and this
        /// problem is fundamental. So we have very weak guarantees for DROP
        /// PART. If any concurrent merge will be assigned then DROP PART will
        /// delete nothing and part will be successfully merged into bigger part.
        ///
        /// dropPart used in the following cases:
        /// 1) Remove empty parts after TTL.
        /// 2) Remove parts after move between shards.
        /// 3) User queries: ALTER TABLE DROP PART 'part_name'.
        ///
        /// In the first case merge of empty part is even better than DROP. In
        /// the second case part UUIDs used to forbid merges for moding parts so
        /// there is no problem with concurrent merges. The third case is quite
        /// rare and we give very weak guarantee: there will be no active part
        /// with this name, but possibly it was merged to some other part.
        if (!drop_range_part_info.isFakeDropRangePart())
            return {};

        return {new_part_name};
    }

    if (type == REPLACE_RANGE)
    {
        Strings res = replace_range_entry->new_part_names;
        auto drop_range_info = MergeTreePartInfo::fromPartName(replace_range_entry->drop_range_part_name, format_version);
        if (auto drop_range = getDropRange(format_version))
            res.emplace_back(*drop_range);
        return res;
    }

    /// Doesn't produce any part.
    if (type == SYNC_PINNED_PART_UUIDS)
        return {};

    /// Doesn't produce any part by itself.
    if (type == CLONE_PART_FROM_SHARD)
        return {};

    return {new_part_name};
}

}
