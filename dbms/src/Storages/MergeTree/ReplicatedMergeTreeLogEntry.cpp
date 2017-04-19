#include <zkutil/Types.h>

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{


void ReplicatedMergeTreeLogEntryData::writeText(WriteBuffer & out) const
{
    out << "format version: 4\n"
        << "create_time: " << LocalDateTime(create_time ? create_time : time(0)) << "\n"
        << "source replica: " << source_replica << '\n'
        << "block_id: " << escape << block_id << '\n';

    switch (type)
    {
        case GET_PART:
            out << "get\n" << new_part_name;
            break;

        case MERGE_PARTS:
            out << "merge\n";
            for (const String & s : parts_to_merge)
                out << s << '\n';
            out << "into\n" << new_part_name;
            out << "\ndeduplicate: " << deduplicate;
            break;

        case DROP_RANGE:
            if (detach)
                out << "detach\n";
            else
                out << "drop\n";
            out << new_part_name;
            break;

        case ATTACH_PART:
            out << "attach\n";
            if (attach_unreplicated)
                out << "unreplicated\n";
            else
                out << "detached\n";
            out << source_part_name << "\ninto\n" << new_part_name;
            break;

        default:
            throw Exception("Unknown log entry type: " + DB::toString<int>(type), ErrorCodes::LOGICAL_ERROR);
    }

    out << '\n';

    if (quorum)
        out << "quorum: " << quorum << '\n';
}

void ReplicatedMergeTreeLogEntryData::readText(ReadBuffer & in)
{
    UInt8 format_version = 0;
    String type_str;

    in >> "format version: " >> format_version >> "\n";

    if (format_version < 1 || format_version > 4)
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
            parts_to_merge.push_back(s);
        }
        in >> new_part_name;
        if (format_version >= 4)
            in >> "\ndeduplicate: " >> deduplicate;
    }
    else if (type_str == "drop" || type_str == "detach")
    {
        type = DROP_RANGE;
        detach = type_str == "detach";
        in >> new_part_name;
    }
    else if (type_str == "attach")
    {
        type = ATTACH_PART;
        String source_type;
        in >> source_type;
        if (source_type == "unreplicated")
            attach_unreplicated = true;
        else if (source_type == "detached")
            attach_unreplicated = false;
        else
            throw Exception("Bad format: expected 'unreplicated' or 'detached', found '" + source_type + "'", ErrorCodes::CANNOT_PARSE_TEXT);
        in >> "\n" >> source_part_name >> "\ninto\n" >> new_part_name;
    }

    in >> "\n";

    /// Optional field.
    if (!in.eof())
        in >> "quorum: " >> quorum >> "\n";
}

String ReplicatedMergeTreeLogEntryData::toString() const
{
    String s;
    {
        WriteBufferFromString out(s);
        writeText(out);
    }
    return s;
}

ReplicatedMergeTreeLogEntry::Ptr ReplicatedMergeTreeLogEntry::parse(const String & s, const zkutil::Stat & stat)
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
