#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/Operators.h>


namespace DB
{

/// Позволяет узнать, куда отправлять запросы, чтобы попасть на реплику.

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
