#include <Poco/Net/StreamSocket.h>

#include <DB/IO/WriteBufferFromPocoSocket.h>
#include <DB/IO/ReadBufferFromPocoSocket.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/copyData.h>


int main(int argc, char ** argv)
try
{
	using namespace DB;

	const char hello[] = "\x00\x11\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65\x20\x63\x6c\x69\x65\x6e\x74\x00\x00\xd9\x86\x03\x00\x07\x64\x65\x66\x61\x75\x6c\x74\x00";

	const char query_header[] = "\x01\x00\x00\x02\x00";

	Poco::Net::StreamSocket socket(Poco::Net::SocketAddress("localhost:9000"));
	WriteBufferFromPocoSocket wb(socket);
	ReadBufferFromPocoSocket rb(socket);

	wb.write(hello, sizeof(hello) - 1);
	wb.next();
	wb.write(query_header, sizeof(query_header) - 1);
	wb.next();

	std::string query_part1 = "SELE";

	for (size_t i = 0; i < 1000000; ++i)
		query_part1 += " TEST ";

	std::string query_part2 = "CT 1";

	for (size_t i = 0; i < 1000000; ++i)
		query_part2 += " PRT2 ";

	size_t total_size = query_part1.size() + query_part2.size();

	writeVarUInt(total_size, wb);
	wb.write(query_part1.data(), query_part1.size());
	wb.next();

	socket.shutdownReceive();

/*	wb.write(query_part2.data(), query_part2.size());
	wb.next();
*/
/*	WriteBufferFromFileDescriptor stderr_wb(STDERR_FILENO);
	copyData(rb, stderr_wb);
	stderr_wb.next();*/

	std::cerr << std::endl;
}
catch (const DB::Exception & e)
{
	std::cerr << e.displayText() << std::endl << e.getStackTrace().toString() << std::endl;
}
