#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <Poco/Net/StreamSocket.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>


/** In a loop it connects to the server and immediately breaks the connection.
  * Using the SO_LINGER option, we ensure that the connection is terminated by sending a RST packet (not FIN).
  * This behavior causes a bug in the TCPServer implementation in the Poco library.
  */
int main(int argc, char ** argv)
try
{
    for (size_t i = 0, num_iters = argc >= 2 ? DB::parse<size_t>(argv[1]) : 1; i < num_iters; ++i)
    {
        std::cerr << ".";

        Poco::Net::SocketAddress address("localhost", 9000);

        int fd = socket(PF_INET, SOCK_STREAM, IPPROTO_IP);

        if (fd < 0)
            DB::throwFromErrno("Cannot create socket", 0);

        linger linger_value;
        linger_value.l_onoff = 1;
        linger_value.l_linger = 0;

        if (0 != setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger_value, sizeof(linger_value)))
            DB::throwFromErrno("Cannot set linger", 0);

        try
        {
            int res = connect(fd, address.addr(), address.length());

            if (res != 0 && errno != EINPROGRESS && errno != EWOULDBLOCK)
            {
                close(fd);
                DB::throwFromErrno("Cannot connect", 0);
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
