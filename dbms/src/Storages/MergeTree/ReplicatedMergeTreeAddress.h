#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

/// Lets you know where to send requests to get to the replica.

struct ReplicatedMergeTreeAddress
{
    String host;
    UInt16 replication_port;
    UInt16 queries_port;
    String database;
    String table;

    ReplicatedMergeTreeAddress() {}
    ReplicatedMergeTreeAddress(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const
    {
        out
            << "host: " << escape << host << '\n'
            << "port: " << replication_port << '\n'
            << "tcp_port: " << queries_port << '\n'
            << "database: " << escape << database << '\n'
            << "table: " << escape << table << '\n';
    }

    void readText(ReadBuffer & in)
    {
        in
            >> "host: " >> escape >> host >> "\n"
            >> "port: " >> replication_port >> "\n"
            >> "tcp_port: " >> queries_port >> "\n"
            >> "database: " >> escape >> database >> "\n"
            >> "table: " >> escape >> table >> "\n";
    }

    String toString() const
    {
        String res;
        {
            WriteBufferFromString out(res);
            writeText(out);
        }
        return res;
    }

    void fromString(const String & str)
    {
        ReadBufferFromString in(str);
        readText(in);
    }
};

}
