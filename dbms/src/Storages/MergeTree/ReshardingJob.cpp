#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

ReshardingJob::ReshardingJob(const std::string & serialized_job)
{
	ReadBufferFromString buf(serialized_job);

	readBinary(database_name, buf);
	readBinary(table_name, buf);
	readBinary(partition, buf);
	readBinary(sharding_key, buf);
	while (!buf.eof())
	{
		std::string path;
		readBinary(path, buf);

		UInt64 weight;
		readBinary(weight, buf);

		paths.emplace_back(path, weight);
	}
}

ReshardingJob::ReshardingJob(const std::string & database_name_, const std::string & table_name_,
	const std::string & partition_, const WeightedZooKeeperPaths & paths_,
	const std::string & sharding_key_)
	: database_name(database_name_),
	table_name(table_name_),
	partition(partition_),
	paths(paths_),
	sharding_key(sharding_key_)
{
}

std::string ReshardingJob::toString() const
{
	std::string serialized_job;
	WriteBufferFromString buf(serialized_job);

	writeBinary(database_name, buf);
	writeBinary(table_name, buf);
	writeBinary(partition, buf);
	writeBinary(sharding_key, buf);
	for (const auto & path : paths)
	{
		writeBinary(path.first, buf);
		writeBinary(path.second, buf);
	}
	buf.next();

	return serialized_job;
}

}
