#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Backups/BackupEntryFromMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

/// Format version 2 appended the "author" field. Entries of both versions are readable.
/// Version 2 is written only when the author is set (see the `persist_mutation_author`
/// setting): entries without an author stay byte-for-byte identical to version 1, so
/// they remain readable by servers that do not know about the "author" field.
static constexpr UInt64 REPLICATED_MUTATION_ENTRY_FORMAT_VERSION_LATEST = 2;

void ReplicatedMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "format version: " << (author.empty() ? 1 : 2) << "\n"
        << "create time: " << LocalDateTime(create_time ? create_time : time(nullptr), DateLUT::serverTimezoneInstance()) << "\n"
        << "source replica: " << source_replica << "\n"
        << "block numbers count: " << block_numbers.size() << "\n";

    for (const auto & kv : block_numbers)
    {
        const String & partition_id = kv.first;
        Int64 number = kv.second;
        out << partition_id << "\t" << number << "\n";
    }

    out << "commands: ";
    commands.writeText(out, /* with_pure_metadata_commands = */ false);
    out << "\n";

    out << "alter version: ";
    out << alter_version;

    if (!author.empty())
        out << "\nauthor: " << escape << author;
}

void ReplicatedMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    UInt64 format_version = 0;
    in >> "format version: " >> format_version >> "\n";
    if (format_version < 1 || format_version > REPLICATED_MUTATION_ENTRY_FORMAT_VERSION_LATEST)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown replicated mutation entry format version: {}", format_version);

    LocalDateTime create_time_dt;
    in >> "create time: " >> create_time_dt >> "\n";
    create_time = makeDateTime(DateLUT::serverTimezoneInstance(),
        create_time_dt.year(), create_time_dt.month(), create_time_dt.day(),
        create_time_dt.hour(), create_time_dt.minute(), create_time_dt.second());

    in >> "source replica: " >> source_replica >> "\n";

    size_t count = 0;
    in >> "block numbers count: " >> count >> "\n";
    for (size_t i = 0; i < count; ++i)
    {
        String partition_id;
        Int64 number = 0;
        in >> partition_id >> "\t" >> number >> "\n";
        block_numbers[partition_id] = number;
    }

    in >> "commands: ";
    commands.readText(in, false);
    if (checkString("\nalter version: ", in))
        in >> alter_version;

    if (format_version >= 2 && checkString("\nauthor: ", in))
        readEscapedStringUntilEOL(author, in);
}

String ReplicatedMergeTreeMutationEntry::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

ReplicatedMergeTreeMutationEntry ReplicatedMergeTreeMutationEntry::parse(const String & str, String znode_name)
{
    ReplicatedMergeTreeMutationEntry res;
    res.znode_name = std::move(znode_name);

    ReadBufferFromString in(str);
    res.readText(in);
    assertEOF(in);

    return res;
}


std::shared_ptr<const IBackupEntry> ReplicatedMergeTreeMutationEntry::backup() const
{
    WriteBufferFromOwnString out;
    out << "block numbers count: " << block_numbers.size() << "\n";

    for (const auto & kv : block_numbers)
    {
        const String & partition_id = kv.first;
        Int64 number = kv.second;
        out << partition_id << "\t" << number << "\n";
    }

    out << "commands: ";
    commands.writeText(out, /* with_pure_metadata_commands = */ false);
    out << "\n";

    return std::make_shared<BackupEntryFromMemory>(out.str());
}


String ReplicatedMergeTreeMutationEntry::getBlockNumbersForLogs() const
{
    WriteBufferFromOwnString out;
    for (const auto & kv : block_numbers)
        out << kv.first << " = " << kv.second << "; ";
    return out.str();
}

}
