#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ReplicatedMergeTreeMutationEntry::writeText(WriteBuffer & out) const
{
    out << "create time: " << LocalDateTime(create_time ? create_time : time(nullptr)) << "\n"
        << "source replica: " << source_replica << "\n"
        << "block number: " << block_number << "\n";

    commands.writeText(out);
}

void ReplicatedMergeTreeMutationEntry::readText(ReadBuffer & in)
{
    LocalDateTime create_time_dt;
    in >> "create time: " >> create_time_dt >> "\n";
    create_time = create_time_dt;

    in >> "source replica: " >> source_replica >> "\n";

    in >> "block number: " >> block_number >> "\n";

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
