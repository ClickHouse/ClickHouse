#include "ReplicatedMergeTreeAddress.h"
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{


void ReplicatedMergeTreeAddress::writeText(WriteBuffer & out) const
{
    out
        << "host: " << escape << host << '\n'
        << "port: " << replication_port << '\n'
        << "tcp_port: " << queries_port << '\n'
        << "database: " << escape << database << '\n'
        << "table: " << escape << table << '\n';
}

void ReplicatedMergeTreeAddress::readText(ReadBuffer & in)
{
    in
        >> "host: " >> escape >> host >> "\n"
        >> "port: " >> replication_port >> "\n"
        >> "tcp_port: " >> queries_port >> "\n"
        >> "database: " >> escape >> database >> "\n"
        >> "table: " >> escape >> table >> "\n";
}

String ReplicatedMergeTreeAddress::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

void ReplicatedMergeTreeAddress::fromString(const String & str)
{
    ReadBufferFromString in(str);
    readText(in);
}
}
