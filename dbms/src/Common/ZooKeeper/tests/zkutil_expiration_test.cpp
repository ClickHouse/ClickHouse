#include <iostream>
#include <Common/ZooKeeper/ZooKeeper.h>
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
                zkutil::Ops ops;
                ops.emplace_back(std::make_unique<zkutil::Op::Create>("/test/zk_expiration_test", "hello", zk.getDefaultACL(), zkutil::CreateMode::Persistent));
                ops.emplace_back(std::make_unique<zkutil::Op::Remove>("/test/zk_expiration_test", -1));

                zkutil::MultiTransactionInfo info;
                zk.tryMultiUnsafe(ops, info);

                std::cout << time(nullptr) - time0 << "s: " << zkutil::ZooKeeper::error2string(info.code) << std::endl;
                try
                {
                    if (info.code != ZOK)
                        std::cout << "Path: " << info.getFailedOp().getPath() << std::endl;
                }
                catch (...)
                {
                    std::cout << DB::getCurrentExceptionMessage(false) << std::endl;
                }

            }

            sleep(1);
        }
    }
    catch (zkutil::KeeperException & e)
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
