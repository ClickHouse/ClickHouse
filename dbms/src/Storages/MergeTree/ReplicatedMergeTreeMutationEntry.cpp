#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ReplicatedMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "format version: 1\n"
        << "create time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "source replica: " << source_replica << "\n"
        << "block numbers count: " << block_numbers.size() << "\n";

    for (const auto & kv : block_numbers)
    {
        const String & partition_id = kv.first;
        Int64 number = kv.second;
        out << partition_id << "\t" << number << "\n";
    }

    out << "mutation commands:\n";
    commands.writeText(out);
}

void ReplicatedMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    in >> "format version: 1\n";

    LocalDateTime create_time_dt;
    in >> "create time: " >> create_time_dt >> "\n";
    create_time = create_time_dt;

    in >> "source replica: " >> source_replica >> "\n";

    size_t count;
    in >> "block numbers count: " >> count >> "\n";
    for (size_t i = 0; i < count; ++i)
    {
        String partition_id;
        Int64 number;
        in >> partition_id >> "\t" >> number >> "\n";
        block_numbers[partition_id] = number;
    }

    in >> "mutation commands:\n";
    commands.readText(in);
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

}
