#pragma once

#include <DB/Core/Types.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/Operators.h>
#include <zkutil/ZooKeeper.h>


namespace DB
{

/** Для реализации функциональности "кворумная запись".
  * Информация о том, на каких репликах появился вставленный кусок данных,
  *  и на скольких репликах он должен быть.
  */
struct ReplicatedMergeTreeQuorumEntry
{
	String part_name;
	size_t required_number_of_replicas;
	std::set<String> replicas;

	ReplicatedMergeTreeQuorumEntry() {}
	ReplicatedMergeTreeQuorumEntry(const String & str)
	{
		fromString(str);
	}

	void writeText(WriteBuffer & out) const
	{
		out << "version: 1\n"
			<< "part_name: " << part_name << "\n"
			<< "required_number_of_replicas: " << required_number_of_replicas << "\n"
			<< "actual_number_of_replicas: " << replicas.size() << "\n"
			<< "replicas:\n";

		for (const auto & replica : replicas)
			out << escape << replica << "\n";
	}

	void readText(ReadBuffer & in)
	{
		size_t actual_number_of_replicas = 0;

		in >> "version: 1\n"
			>> "part_name: " >> part_name >> "\n"
			>> "required_number_of_replicas: " >> required_number_of_replicas >> "\n"
			>> "actual_number_of_replicas: " >> actual_number_of_replicas >> "\n"
			>> "replicas:\n";

		for (size_t i = 0; i < actual_number_of_replicas; ++i)
		{
			String replica;
			in >> escape >> replica >> "\n";
			replicas.insert(replica);
		}
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
