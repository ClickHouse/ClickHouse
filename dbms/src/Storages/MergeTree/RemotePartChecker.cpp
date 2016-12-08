#include <DB/Storages/MergeTree/RemotePartChecker.h>
#include <DB/Storages/MergeTree/ReshardingWorker.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace RemotePartChecker
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
	return "RemotePartChecker:" + node_id;
}

}

Service::Service(StoragePtr & storage_)
	: owned_storage{storage_}, data{static_cast<StorageReplicatedMergeTree &>(*storage_).getData()}
{
}

std::string Service::getId(const std::string & node_id) const
{
	return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out)
{
	auto part_name = params.get("part");
	auto hash = params.get("hash");

	Status status = Status::OK;

	try
	{
		auto part_path = data.getFullPath() + "detached/" + part_name;
		if (!Poco::File{part_path}.exists())
			status = Status::NOT_FOUND;
		else
		{
			auto computed_hash = ReshardingWorker::computeHashFromPart(part_path);
			if (computed_hash != hash)
				status = Status::INCONSISTENT;
		}
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
		status = Status::ERROR;
	}

	writeBinary(static_cast<UInt8>(status), out);
	out.next();
}

Status Client::check(const std::string & part_name, const std::string & hash,
	const InterserverIOEndpointLocation & to_location)
{
	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", getEndpointId(to_location.name) },
		{"compress", "false"},
		{"part", part_name},
		{"hash", hash}
	};

	ReadBufferFromHTTP in{to_location.host, to_location.port, "", params};

	UInt8 val;
	readBinary(val, in);
	assertEOF(in);
	return static_cast<Status>(val);
}

}

}
