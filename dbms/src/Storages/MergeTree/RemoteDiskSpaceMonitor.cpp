#include <DB/Storages/MergeTree/RemoteDiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/DiskSpaceMonitor.h>
#include <DB/Interpreters/Context.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
}

namespace RemoteDiskSpaceMonitor
{

namespace
{

std::string getEndpointId(const std::string & node_id)
{
	return "RemoteDiskSpaceMonitor:" + node_id;
}

}

Service::Service(const Context & context_)
	: context{context_}
{
}

std::string Service::getId(const std::string & node_id) const
{
	return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & body, WriteBuffer & out)
{
	if (is_cancelled)
		throw Exception{"RemoteDiskSpaceMonitor service terminated", ErrorCodes::ABORTED};

	size_t free_space = DiskSpaceMonitor::getUnreservedFreeSpace(context.getPath());
	writeBinary(free_space, out);
	out.next();
}

size_t Client::getFreeSpace(const InterserverIOEndpointLocation & location) const
{
	ReadBufferFromHTTP::Params params =
	{
		{"endpoint", getEndpointId(location.name) },
		{"compress", "false"}
	};

	ReadBufferFromHTTP in{location.host, location.port, params};

	size_t free_disk_space;
	readBinary(free_disk_space, in);
	assertEOF(in);

	return free_disk_space;
}

}

}
