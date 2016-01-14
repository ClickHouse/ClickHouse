#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <Poco/Net/StreamSocket.h>
#include <DB/Common/Exception.h>
#include <DB/IO/ReadHelpers.h>


/** В цикле соединяется с сервером и сразу же разрывает соединение.
  * С помощью опции SO_LINGER добиваемся, что соединение разрывается путём отправки пакета RST (не FIN).
  * Такое поведение вызывает баг в реализации TCPServer в библиотеке Poco.
  */

int main(int argc, char ** argv)
try
{
	for (size_t i = 0; i < DB::parse<size_t>(argv[1]); ++i)
	{
		std::cerr << ".";

		Poco::Net::SocketAddress address("127.0.0.1", 9000);

		int fd = socket(PF_INET, SOCK_STREAM, IPPROTO_IP);

		if (fd < 0)
			DB::throwFromErrno("Cannot create socket");

		linger linger_value;
		linger_value.l_onoff = 1;
		linger_value.l_linger = 0;

		if (0 != setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger_value, sizeof(linger_value)))
			DB::throwFromErrno("Cannot set linger");

		try
		{
			int res = connect(fd, address.addr(), address.length());

			if (res != 0 && errno != EINPROGRESS && errno != EWOULDBLOCK)
			{
				close(fd);
				DB::throwFromErrno("Cannot connect");
			}

			close(fd);
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << e.displayText() << "\n";
		}
	}

	std::cerr << "\n";
}
catch (const Poco::Exception & e)
{
	std::cerr << e.displayText() << "\n";
}
