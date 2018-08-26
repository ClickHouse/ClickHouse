#include <iostream>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Poco/ConsoleChannel.h>
#include <Common/Exception.h>


/// Проверяет, какие ошибки выдает ZooKeeper при попытке сделать какую-нибудь операцию через разное время после истечения сессии.
/// Спойлер: multi иногда падает с segfault, а до этого фейлится с marshalling error.
///          create всегда фейлится с invalid zhandle state.

int main(int argc, char ** argv)
{
    try
    {
        if (argc != 2)
        {
            std::cerr << "usage: " << argv[0] << " hosts" << std::endl;
            return 2;
        }

        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
        Logger::root().setChannel(channel);
        Logger::root().setLevel("trace");

        zkutil::ZooKeeper zk(argv[1]);
        std::string unused;
        zk.tryCreate("/test", "", zkutil::CreateMode::Persistent, unused);

        std::cerr << "Please run `./nozk.sh && sleep 40s && ./yeszk.sh`" << std::endl;

        time_t time0 = time(nullptr);

        while (true)
        {
            {
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCreateRequest("/test/zk_expiration_test", "hello", zkutil::CreateMode::Persistent));
                ops.emplace_back(zkutil::makeRemoveRequest("/test/zk_expiration_test", -1));

                Coordination::Responses responses;
                int32_t code = zk.tryMultiNoThrow(ops, responses);

                std::cout << time(nullptr) - time0 << "s: " << zkutil::ZooKeeper::error2string(code) << std::endl;
                try
                {
                    if (code)
                        std::cout << "Path: " << zkutil::KeeperMultiException(code, ops, responses).getPathForFirstFailedOp() << std::endl;
                }
                catch (...)
                {
                    std::cout << DB::getCurrentExceptionMessage(false) << std::endl;
                }

            }

            sleep(1);
        }
    }
    catch (Coordination::Exception &)
    {
        std::cerr << "KeeperException: " << DB::getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }
    catch (...)
    {
        std::cerr << "Some exception: " << DB::getCurrentExceptionMessage(true) << std::endl;
        return 2;
    }

    return 0;
}
