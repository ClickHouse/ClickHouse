#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <thread>
#include <atomic>
#include <Poco/Net/StreamSocket.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <IO/ReadHelpers.h>


/** In a loop it connects to the server and immediately breaks the connection.
  * Using the SO_LINGER option, we ensure that the connection is terminated by sending a RST packet (not FIN).
  * Long time ago this behavior caused a bug in the TCPServer implementation in the Poco library.
  */
int main(int argc, char ** argv)
try
{
    size_t num_iterations = 1;
    size_t num_threads = 1;
    std::string host = "localhost";
    uint16_t port = 9000;

    if (argc >= 2)
        num_iterations = DB::parse<size_t>(argv[1]);

    if (argc >= 3)
        num_threads = DB::parse<size_t>(argv[2]);

    if (argc >= 4)
        host = argv[3];

    if (argc >= 5)
        port = DB::parse<uint16_t>(argv[4]);

    std::atomic_bool cancel{false};
    std::vector<std::thread> threads(num_threads);
    for (auto & thread : threads)
    {
        thread = std::thread([&]
        {
            for (size_t i = 0; i < num_iterations && !cancel.load(std::memory_order_relaxed); ++i)
            {
                std::cerr << ".";

                Poco::Net::SocketAddress address(host, port);

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
                    Stopwatch watch;

                    int res = connect(fd, address.addr(), address.length());

                    if (res != 0 && errno != EINPROGRESS && errno != EWOULDBLOCK)
                    {
                        close(fd);
                        DB::throwFromErrno("Cannot connect", 0);
                    }

                    close(fd);

                    if (watch.elapsedSeconds() > 0.1)
                    {
                        std::cerr << watch.elapsedSeconds() << "\n";
                        cancel = true;
                        break;
                    }
                }
                catch (const Poco::Exception & e)
                {
                    std::cerr << e.displayText() << "\n";
                }
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    std::cerr << "\n";
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
