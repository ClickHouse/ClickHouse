#include <Common/ZooKeeper/Types.h>

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}


void ReplicatedMergeTreeLogEntryData::writeText(WriteBuffer & out) const
{
    UInt8 format_version = 4;

    /// Conditionally bump format_version only when uuid has been assigned.
    /// If some other feature requires bumping format_version to >= 5 then this code becomes no-op.
    if (new_part_uuid != UUIDHelpers::Nil)
        format_version = std::max(format_version, static_cast<UInt8>(5));

    out << "format version: " << format_version << "\n"
        << "create_time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "source replica: " << source_replica << '\n'
        << "block_id: " << escape << block_id << '\n';

    switch (type)
    {
        case GET_PART:
            out << "get\n" << new_part_name;
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

        default:
            throw Exception("Unknown log entry type: " + DB::toString<int>(type), ErrorCodes::LOGICAL_ERROR);
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

    if (format_version < 1 || format_version > 5)
        throw Exception("Unknown ReplicatedMergeTreeLogEntry format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

    if (format_version >= 2)
    {
        LocalDateTime create_time_dt;
        in >> "create_time: " >> create_time_dt >> "\n";
        create_time = create_time_dt;
    }

    in >> "source replica: " >> source_replica >> "\n";

    if (format_version >= 3)
    {
        in >> "block_id: " >> escape >> block_id >> "\n";
    }

    in >> type_str >> "\n";

    bool trailing_newline_found = false;
    if (type_str == "get")
    {
        type = GET_PART;
        in >> new_part_name;
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

        if (format_version >= 4)
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

}
