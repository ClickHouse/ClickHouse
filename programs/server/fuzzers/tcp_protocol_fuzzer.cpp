#include <cstdint>
#include <future>
#include <thread>
#include <utility>
#include <vector>
#include <iostream>
#include <chrono>

#include <Poco/Net/PollSet.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <Daemon/BaseDaemon.h>
#include <Interpreters/Context.h>


int mainEntryClickHouseServer(int argc, char ** argv);

static std::string clickhouse("clickhouse-server");
static std::vector<char *> args{clickhouse.data()};
static std::future<int> main_app;

static std::string s_host("0.0.0.0");
static char * host = s_host.data();
static int64_t port = 9000;

using namespace std::chrono_literals;

void on_exit()
{
    BaseDaemon::terminate();
    main_app.wait();
}

extern "C"
int LLVMFuzzerInitialize(int * argc, char ***argv)
{
    for (int i = 1; i < *argc; ++i)
    {
        if ((*argv)[i][0] == '-')
        {
            if ((*argv)[i][1] == '-')
                args.push_back((*argv)[i]);
            else
            {
                if (strncmp((*argv)[i], "-host=", 6) == 0)
                {
                    host = (*argv)[i] + 6;
                }
                else if (strncmp((*argv)[i], "-port=", 6) == 0)
                {
                    char * p_end = nullptr;
                    port = strtol((*argv)[i] + 6, &p_end, 10);
                }
            }
        }
    }

    args.push_back(nullptr);

    main_app = std::async(std::launch::async, mainEntryClickHouseServer, args.size() - 1, args.data());

    while (!DB::Context::getGlobalContextInstance() || !DB::Context::getGlobalContextInstance()->isServerCompletelyStarted())
    {
        std::this_thread::sleep_for(100ms);
        if (main_app.wait_for(0s) == std::future_status::ready)
            exit(-1);
    }

    atexit(on_exit);

    return 0;
}

extern "C"
int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        if (main_app.wait_for(0s) == std::future_status::ready)
            return -1;

        if (size == 0)
            return -1;

        Poco::Net::SocketAddress address(host, port);
        Poco::Net::StreamSocket socket;

        socket.connectNB(address);

        Poco::Net::PollSet ps;
        ps.add(socket, Poco::Net::PollSet::POLL_READ | Poco::Net::PollSet::POLL_WRITE);

        std::vector<char> buf(1048576);
        size_t sent = 0;
        while (true)
        {
            auto m = ps.poll(Poco::Timespan(1000000));
            if (m.empty())
                continue;
            if (m.begin()->second & Poco::Net::PollSet::POLL_READ)
            {
                if (int n = socket.receiveBytes(buf.data(), static_cast<int>(buf.size())); n == 0)
                {
                    socket.close();
                    break;
                }

                continue;
            }

            if (sent < size && m.begin()->second & Poco::Net::PollSet::POLL_WRITE)
            {
                sent += socket.sendBytes(data + sent, static_cast<int>(size - sent));
                if (sent == size)
                {
                    socket.shutdownSend();
                    continue;
                }
            }
        }
    }
    catch (...)
    {
    }

    return 0;
}
