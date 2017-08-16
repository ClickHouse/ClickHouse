#include <iostream>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Poco/ConsoleChannel.h>


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

                int code;
                try
                {
                    code = zk.tryMulti(ops);std::string unused;
                    //code = zk.tryCreate("/test", "", zkutil::CreateMode::Persistent, unused);
                }
                catch (zkutil::KeeperException & e)
                {
                    code = e.code;
                }

                std::cout << time(0) - time0 << "s: " << zkutil::ZooKeeper::error2string(code) << std::endl;
            }

            sleep(1);
        }
    }
    catch (zkutil::KeeperException & e)
    {
        std::cerr << "KeeperException: " << e.displayText() << std::endl;
        return 1;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 2;
    }

    return 0;
}
