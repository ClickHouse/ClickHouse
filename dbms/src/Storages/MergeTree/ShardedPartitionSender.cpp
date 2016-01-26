#include <DB/Storages/MergeTree/ShardedPartitionSender.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
}

namespace
{

std::string glue(const std::vector<std::string> & names, char delim)
{
	std::string res;
	bool is_first = true;

	for (const auto & name : names)
	{
		if (is_first)
			is_first = false;
		else
			res.append(1, delim);
		res.append(name);
	}

	return res;
}

}

namespace ShardedPartitionSender
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
	return "ShardedPartitionSender:" + node_id;
}

}

Service::Service(StorageReplicatedMergeTree & storage_)
	: storage(storage_)
{
}

std::string Service::getId(const std::string & node_id) const
{
	return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out)
{
	if (is_cancelled)
		throw Exception("ShardedPartitionSender service terminated", ErrorCodes::ABORTED);

	InterserverIOEndpointLocation from_location(params.get("from_location"));
	std::string glued_parts = params.get("parts");
	size_t shard_no = std::stoul(params.get("shard"));

	std::vector<std::string> parts;
	boost::split(parts, glued_parts, boost::is_any_of(","));

	for (const auto & part_name : parts)
	{
		if (is_cancelled)
			throw Exception("ShardedPartitionSender service terminated", ErrorCodes::ABORTED);

		MergeTreeData::MutableDataPartPtr part = storage.fetcher.fetchShardedPart(from_location, part_name, shard_no);
		part->is_temp = false;
		const std::string new_name = "detached/" + part_name;
		Poco::File(storage.full_path + part->name).renameTo(storage.full_path + new_name);
	}

	bool flag = true;
	writeBinary(flag, out);
	out.next();
}

bool Client::send(const InterserverIOEndpointLocation & to_location, const InterserverIOEndpointLocation & from_location,
	const std::vector<std::string> & parts, size_t shard_no)
{
	std::string glued_parts = glue(parts, ',');

	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", getEndpointId(to_location.name)},
		{"from_location", from_location.toString()},
		{"compress", "false"},
		{"parts", glued_parts},
		{"shard", toString(shard_no)}
	};

	ReadBufferFromHTTP in(to_location.host, to_location.port, params);

	bool flag;
	readBinary(flag, in);
	assertEOF(in);

	return flag;
}

}

}
