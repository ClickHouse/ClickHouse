#include <Common/ZooKeeper/Types.h>

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <google/protobuf/text_format.h>
#include <ReplicationLogEntry.pb.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartType::Value partTypeFromProtoEnum(Protobuf::ReplicatedMergeTree::PartType t);
Protobuf::ReplicatedMergeTree::PartType partTypeToProtoEnum(MergeTreeDataPartType t);

String ReplicatedMergeTreeLogEntryData::toString() const
{
    return toV5ProtoTextString();
}

String ReplicatedMergeTreeLogEntryData::toV5ProtoTextString() const
{
    Protobuf::ReplicatedMergeTree::LogEntry log_entry;
    log_entry.set_create_time(create_time ? create_time : time(nullptr));
    log_entry.set_source_replica(source_replica);

    switch (type)
    {

        case GET_PART:
        {
            auto * get_part = log_entry.mutable_get_part();
            get_part->set_new_part_name(new_part_name);
            get_part->set_block_id(block_id);
            get_part->set_quorum(quorum);

            if (new_part_type == MergeTreeDataPartType::UNKNOWN)
                get_part->set_part_type(partTypeToProtoEnum(MergeTreeDataPartType::WIDE));
            else
                get_part->set_part_type(partTypeToProtoEnum(new_part_type));
        }
            break;

        case MERGE_PARTS:
        {
            auto * merge_parts = log_entry.mutable_merge_parts();

            for (const String & s : source_parts)
                merge_parts->add_source_parts(s);

            merge_parts->set_new_part_name(new_part_name);
            merge_parts->set_deduplicate(deduplicate);

            Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type proto_merge_type;
            switch (merge_type) {
                case MergeType::REGULAR:
                    proto_merge_type = Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_REGULAR;
                    break;
                case MergeType::TTL_DELETE:
                    proto_merge_type = Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_TTL_DELETE;
                    break;
                case MergeType::TTL_RECOMPRESS:
                    proto_merge_type = Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_TTL_RECOMPRESS;
                    break;
            }
            merge_parts->set_merge_type(proto_merge_type);

            /// Looks like it is not always initialized, handling like in txt format.
            if (new_part_type == MergeTreeDataPartType::UNKNOWN)
                merge_parts->set_part_type(partTypeToProtoEnum(MergeTreeDataPartType::WIDE));
            else
                merge_parts->set_part_type(partTypeToProtoEnum(new_part_type));
        }
            break;

        case DROP_RANGE:
        {
            auto * drop_range = log_entry.mutable_drop_range();
            drop_range->set_part_name(new_part_name);
            drop_range->set_detach(detach);
        }
            break;

        case REPLACE_RANGE:
        {
            auto * replace_range = log_entry.mutable_replace_range();
            replace_range->set_drop_range_part_name(replace_range_entry->drop_range_part_name);
            replace_range->set_from_database(replace_range_entry->from_database);
            replace_range->set_from_table(replace_range_entry->from_table);

            for (const String & p : replace_range_entry->src_part_names)
                replace_range->add_src_part_names(p);

            for (const String & p : replace_range_entry->new_part_names)
                replace_range->add_new_part_names(p);

            for (const String & p : replace_range_entry->part_names_checksums)
                replace_range->add_part_names_checksums(p);

            replace_range->set_columns_version(replace_range_entry->columns_version);
        }
            break;

        case MUTATE_PART:
        {
            auto * mutate_part = log_entry.mutable_mutate_part();
            mutate_part->set_src_part_name(source_parts.at(0));
            mutate_part->set_dst_part_name(new_part_name);
            mutate_part->set_alter_version(alter_version);
        }
            break;

        case ALTER_METADATA:
        {
            auto * alter_metadata = log_entry.mutable_alter_metadata();
            alter_metadata->set_alter_version(alter_version);
            alter_metadata->set_have_mutation(have_mutation);
            alter_metadata->set_columns(columns_str);
            alter_metadata->set_metadata(metadata_str);
        }
            break;


            /// These do not exist in protobuf implementation at all.
        case EMPTY: [[fallthrough]];
        case CLEAR_COLUMN: [[fallthrough]];
        case CLEAR_INDEX:
            __builtin_unreachable();
    }

    WriteBufferFromOwnString out;

    out << "format version: " << LOG_PROTOTEXT_VERSION << "\n";

    String proto_txt;
    if (!google::protobuf::TextFormat::PrintToString(log_entry, &proto_txt))
        throw Exception("Failed to serialize proto message to text", ErrorCodes::LOGICAL_ERROR);
    out << proto_txt;

    return out.str();
}


String ReplicatedMergeTreeLogEntryData::toV4ManualTextString() const
{
    WriteBufferFromOwnString out;

    out << "format version: 4\n"
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

    return out.str();
}

void ReplicatedMergeTreeLogEntryData::fromV5ProtoText(ReadBuffer & in)
{
    String proto_txt;
    readStringUntilEOF(proto_txt, in);

    Protobuf::ReplicatedMergeTree::LogEntry log_entry;
    if (!google::protobuf::TextFormat::ParseFromString(proto_txt, &log_entry))
        throw Exception("Failed to deserialize proto message from text: " + proto_txt, ErrorCodes::LOGICAL_ERROR);

    create_time = LocalDateTime(log_entry.create_time());
    source_replica = log_entry.source_replica();

    switch (log_entry.EntryPayload_case())
    {
        case Protobuf::ReplicatedMergeTree::LogEntry::kGetPart:
        {
            const auto & get_part = log_entry.get_part();

            type = GET_PART;
            new_part_name = get_part.new_part_name();
            block_id = get_part.block_id();
            quorum = get_part.quorum();
            new_part_type = partTypeFromProtoEnum(get_part.part_type());
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::kMergeParts:
        {
            const auto & merge_parts = log_entry.merge_parts();

            type = MERGE_PARTS;
            source_parts.assign(merge_parts.source_parts().begin(), merge_parts.source_parts().end());
            new_part_name = merge_parts.new_part_name();
            deduplicate = merge_parts.deduplicate();
            switch (merge_parts.merge_type())
            {
                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_UNKNOWN:
                    throw Exception("Unexpected empty merge_type", ErrorCodes::UNSUPPORTED_METHOD);
                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_REGULAR:
                    merge_type = MergeType::REGULAR;
                    break;
                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_TTL_DELETE:
                    merge_type = MergeType::TTL_DELETE;
                    break;
                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_TTL_RECOMPRESS:
                    merge_type = MergeType::TTL_RECOMPRESS;
                    break;

                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_LogEntry_MergeParts_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
                    [[fallthrough]];
                case Protobuf::ReplicatedMergeTree::LogEntry_MergeParts_Type_LogEntry_MergeParts_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
                    __builtin_unreachable();
            }
            new_part_type = partTypeFromProtoEnum(merge_parts.part_type());
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::kMutatePart:
        {
            const auto & mutate_part = log_entry.mutate_part();

            type = MUTATE_PART;
            source_parts.emplace_back(mutate_part.src_part_name());
            new_part_name = mutate_part.dst_part_name();
            alter_version = mutate_part.alter_version();
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::kAlterMetadata:
        {
            const auto & alter_metadata = log_entry.alter_metadata();

            type = ALTER_METADATA;
            alter_version = alter_metadata.alter_version();
            have_mutation = alter_metadata.have_mutation();
            columns_str = alter_metadata.columns();
            metadata_str = alter_metadata.metadata();
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::kDropRange: {
            const auto & drop_range = log_entry.drop_range();

            type = DROP_RANGE;
            new_part_name = drop_range.part_name();
            detach = drop_range.detach();
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::kReplaceRange:
        {
            const auto & replace_range = log_entry.replace_range();

            type = REPLACE_RANGE;
            replace_range_entry = std::make_shared<ReplaceRangeEntry>();
            replace_range_entry->drop_range_part_name = replace_range.drop_range_part_name();
            replace_range_entry->from_database = replace_range.from_database();
            replace_range_entry->from_table = replace_range.from_table();
            replace_range_entry->src_part_names.assign(replace_range.src_part_names().begin(), replace_range.src_part_names().end());
            replace_range_entry->new_part_names.assign(replace_range.new_part_names().begin(), replace_range.new_part_names().end());
            replace_range_entry->part_names_checksums.assign(
                replace_range.part_names_checksums().begin(), replace_range.part_names_checksums().end());
            replace_range_entry->columns_version = replace_range.columns_version();
        }
        break;

        case Protobuf::ReplicatedMergeTree::LogEntry::ENTRYPAYLOAD_NOT_SET:
            throw Exception("Unexpected empty EntryPayload", ErrorCodes::UNSUPPORTED_METHOD);
    }
}

void ReplicatedMergeTreeLogEntryData::fromV4ManualText(ReadBuffer & in, UInt8 format_version)
{
    in >> "format version: " >> format_version >> "\n";

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

    String type_str;
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
            in >> "\n";
            if (checkString("merge_type: ", in))
            {
                UInt64 value;
                in >> value;
                merge_type = checkAndGetMergeType(value);
            }
            else
                trailing_newline_found = true;
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

        in >> "\n";

        if (in.eof())
            trailing_newline_found = true;
        else if (checkString("alter_version\n", in))
            in >> alter_version;
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

ReplicatedMergeTreeLogEntry::Ptr ReplicatedMergeTreeLogEntry::parse(const String & s, const Coordination::Stat & stat)
{
    ReadBufferFromString in(s);
    Ptr res = std::make_shared<ReplicatedMergeTreeLogEntry>();

    UInt8 format_version = 0;
    in >> "format version: " >> format_version >> "\n";

    if (format_version >= 1 && format_version < LOG_PROTOTEXT_VERSION)
        res->fromV4ManualText(in, format_version);
    else if (format_version == LOG_PROTOTEXT_VERSION)
        res->fromV5ProtoText(in);
    else
        throw Exception("Unknown ReplicatedMergeTreeLogEntry format version: " + DB::toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

    assertEOF(in);

    if (!res->create_time)
        res->create_time = stat.ctime / 1000;

    return res;
}


Protobuf::ReplicatedMergeTree::PartType partTypeToProtoEnum(MergeTreeDataPartType t)
{
    if (t == MergeTreeDataPartType::WIDE)
        return Protobuf::ReplicatedMergeTree::WIDE;
    else if (t == MergeTreeDataPartType::COMPACT)
        return Protobuf::ReplicatedMergeTree::COMPACT;
    else if (t == MergeTreeDataPartType::IN_MEMORY)
        return Protobuf::ReplicatedMergeTree::IN_MEMORY;

    throw Exception("Part type not handled: " + t.toString(), ErrorCodes::LOGICAL_ERROR);
}

MergeTreeDataPartType::Value partTypeFromProtoEnum(Protobuf::ReplicatedMergeTree::PartType t)
{
    switch (t)
    {
        case Protobuf::ReplicatedMergeTree::WIDE: return MergeTreeDataPartType::WIDE;
        case Protobuf::ReplicatedMergeTree::COMPACT: return MergeTreeDataPartType::COMPACT;
        case Protobuf::ReplicatedMergeTree::IN_MEMORY: return MergeTreeDataPartType::IN_MEMORY;

        case Protobuf::ReplicatedMergeTree::UNKNOWN: throw Exception("Did not expect UNKNOWN part type", ErrorCodes::UNSUPPORTED_METHOD);

        case Protobuf::ReplicatedMergeTree::PartType_INT_MIN_SENTINEL_DO_NOT_USE_: [[fallthrough]];
        case Protobuf::ReplicatedMergeTree::PartType_INT_MAX_SENTINEL_DO_NOT_USE_:
            __builtin_unreachable();
    }
}


}
