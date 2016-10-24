#include <unistd.h>

#include <DB/Interpreters/ClientInfo.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/Defines.h>
#include <DB/Common/getFQDNOrHostName.h>
#include <common/ClickHouseRevision.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_CLIENT_INFO_VERSION;
}


/// Change when format is changed.
static const UInt32 client_info_version = 1;


void ClientInfo::write(WriteBuffer & out) const
{
	writeVarUInt(client_info_version, out);

	writeBinary(UInt8(query_kind), out);
	if (empty())
		return;

	writeBinary(initial_user, out);
	writeBinary(initial_query_id, out);
	writeBinary(initial_address.toString(), out);

	writeBinary(UInt8(interface), out);

	if (interface == Interface::TCP)
	{
		writeBinary(os_user, out);
		writeBinary(client_hostname, out);
		writeBinary(client_name, out);
		writeVarUInt(client_version_major, out);
		writeVarUInt(client_version_minor, out);
		writeVarUInt(client_revision, out);
	}
	else if (interface == Interface::HTTP)
	{
		writeBinary(UInt8(http_method), out);
		writeBinary(http_user_agent, out);
	}
}


void ClientInfo::read(ReadBuffer & in)
{
	UInt32 read_client_info_version = 0;
	readVarUInt(read_client_info_version, in);

	if (client_info_version != read_client_info_version)
		throw Exception("Unknown version of ClientInfo: " + toString(read_client_info_version), ErrorCodes::UNKNOWN_CLIENT_INFO_VERSION);

	UInt8 read_query_kind = 0;
	readBinary(read_query_kind, in);
	query_kind = QueryKind(read_query_kind);
	if (empty())
		return;

	readBinary(initial_user, in);
	readBinary(initial_query_id, in);

	String initial_address_string;
	readBinary(initial_address_string, in);
	initial_address = Poco::Net::SocketAddress(initial_address_string);

	UInt8 read_interface = 0;
	readBinary(read_interface, in);
	interface = Interface(read_interface);

	if (interface == Interface::TCP)
	{
		readBinary(os_user, in);
		readBinary(client_hostname, in);
		readBinary(client_name, in);
		readVarUInt(client_version_major, in);
		readVarUInt(client_version_minor, in);
		readVarUInt(client_revision, in);
	}
	else if (interface == Interface::HTTP)
	{
		UInt8 read_http_method = 0;
		readBinary(read_http_method, in);
		http_method = HTTPMethod(read_http_method);

		readBinary(http_user_agent, in);
	}
}


void ClientInfo::fillOSUserHostNameAndVersionInfo()
{
	os_user.resize(L_cuserid + 1, '\0');
	if (0 != getlogin_r(&os_user[0], L_cuserid))
		os_user.resize(strlen(os_user.data()));
	else
		os_user.clear();	/// Don't mind if we cannot determine user login.

	client_hostname = getFQDNOrHostName();

	client_version_major = DBMS_VERSION_MAJOR;
	client_version_minor = DBMS_VERSION_MINOR;
	client_revision = ClickHouseRevision::get();
}


}
