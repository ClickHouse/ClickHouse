#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <thread>
#include <atomic>
#include <Poco/Net/StreamSocket.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>


/** In a loop it connects to the server and immediately breaks the connection.
  * Using the SO_LINGER option, we ensure that the connection is terminated by sending a RST packet (not FIN).
  * Long time ago this behavior caused a bug in the TCPServer implementation in the Poco library.
  */
int main(int argc, char ** argv)
try
{
    using namespace DB;

    size_t num_iterations = 1;
    size_t num_threads = 1;
    std::string host = "localhost";
    uint16_t port = 9000;

    if (argc >= 2)
        num_iterations = parse<size_t>(argv[1]);

    if (argc >= 3)
        num_threads = parse<size_t>(argv[2]);

    if (argc >= 4)
        host = argv[3];

    if (argc >= 5)
        port = parse<uint16_t>(argv[4]);

    // WriteBufferFromFileDescriptor out(STDERR_FILENO);


    std::atomic_bool cancel{false};
    std::vector<std::thread> threads(num_threads);
    for (auto & thread : threads)
    {
        thread = std::thread([&]
        {
            for (size_t i = 0; i < num_iterations && !cancel.load(std::memory_order_relaxed); ++i)
            {
                std::cerr << ".";

                try
                {
                    Poco::Net::SocketAddress address(host, port);
                    Poco::Net::StreamSocket socket;
                    //socket.setLinger(1, 0);

                    socket.connectNB(address);
                    if (!socket.poll(Poco::Timespan(1000000),
                        Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_WRITE | Poco::Net::Socket::SELECT_ERROR))
                    {
                        /// Allow to debug the server.
/*                        auto command = ShellCommand::execute("kill -STOP $(pidof clickhouse-server)");
                        copyData(command->err, out);
                        copyData(command->out, out);
                        command->wait();*/

                        std::cerr << "Timeout\n";
/*                        cancel = true;
                        break;*/
                    }
                }
                catch (const Poco::Exception & e)
                {
                    std::cerr << e.displayText() << "\n";
                    cancel = true;
                    break;
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
