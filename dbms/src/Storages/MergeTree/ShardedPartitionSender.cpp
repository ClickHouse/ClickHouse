#include <DB/Storages/MergeTree/ShardedPartitionSender.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
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
	: storage(storage_), log(&Logger::get("ShardedPartitionSender::Service"))
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
	std::string part_name = params.get("part");
	size_t shard_no = std::stoul(params.get("shard"));

	if (is_cancelled)
		throw Exception("ShardedPartitionSender service terminated", ErrorCodes::ABORTED);

	MergeTreeData::MutableDataPartPtr part = storage.fetcher.fetchShardedPart(from_location, part_name, shard_no);
	part->is_temp = false;

	const std::string old_part_path = storage.full_path + part->name;
	const std::string new_part_path = storage.full_path + "detached/" + part_name;

	Poco::File new_part_dir(new_part_path);
	if (new_part_dir.exists())
	{
		LOG_WARNING(log, "Directory " + new_part_path + " already exists. Removing.");
		new_part_dir.remove(true);
	}

	Poco::File(old_part_path).renameTo(new_part_path);

	bool flag = true;
	writeBinary(flag, out);
	out.next();
}

Client::Client()
	: log(&Logger::get("ShardedPartitionSender::Client"))
{
}

bool Client::send(const InterserverIOEndpointLocation & to_location, const InterserverIOEndpointLocation & from_location,
	const std::string & part, size_t shard_no)
{
	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", getEndpointId(to_location.name)},
		{"from_location", from_location.toString()},
		{"compress", "false"},
		{"part", part},
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
