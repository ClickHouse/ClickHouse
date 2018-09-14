#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Event.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/typeid_cast.h>
#include <iostream>
#include <boost/algorithm/string.hpp>


using namespace Coordination;


int main(int argc, char ** argv)
try
{
    if (argc < 2)
    {
        std::cerr << "Usage: ./zkutil_test_commands_new_lib host:port,host:port...\n";
        return 1;
    }

    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    std::string addresses_arg = argv[1];
    std::vector<std::string> addresses_strings;
    boost::split(addresses_strings, addresses_arg, boost::is_any_of(","));
    ZooKeeper::Addresses addresses;
    addresses.reserve(addresses_strings.size());
    for (const auto & address_string : addresses_strings)
        addresses.emplace_back(address_string);

    ZooKeeper zk(addresses, {}, {}, {}, {5, 0}, {0, 50000}, {0, 50000});

    Poco::Event event(true);

    std::cout << "create\n";

    zk.create("/test", "old", false, false, {},
        [&](const CreateResponse & response)
        {
            if (response.error)
                std::cerr << "Error (create) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Created path: " << response.path_created << '\n';

            //event.set();
        });

    //event.wait();

    std::cout << "get\n";

    zk.get("/test",
        [&](const GetResponse & response)
        {
            if (response.error)
                std::cerr << "Error (get) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Value: " << response.data << '\n';

            //event.set();
        },
        [](const WatchResponse & response)
        {
            if (response.error)
                std::cerr << "Watch (get) on /test, Error " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Watch (get) on /test, path: " << response.path << ", type: " << response.type << '\n';
        });

    //event.wait();

    std::cout << "set\n";

    zk.set("/test", "new", -1,
        [&](const SetResponse & response)
        {
            if (response.error)
                std::cerr << "Error (set) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Set\n";

            //event.set();
        });

    //event.wait();

    std::cout << "list\n";

    zk.list("/",
        [&](const ListResponse & response)
        {
            if (response.error)
                std::cerr << "Error (list) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
            {
                std::cerr << "Children:\n";
                for (const auto & name : response.names)
                    std::cerr << name << "\n";
            }

            //event.set();
        },
        [](const WatchResponse & response)
        {
            if (response.error)
                std::cerr << "Watch (list) on /, Error " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Watch (list) on /, path: " << response.path << ", type: " << response.type << '\n';
        });

    //event.wait();

    std::cout << "exists\n";

    zk.exists("/test",
        [&](const ExistsResponse & response)
        {
            if (response.error)
                std::cerr << "Error (exists) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Exists\n";

            //event.set();
        },
        [](const WatchResponse & response)
        {
            if (response.error)
                std::cerr << "Watch (exists) on /test, Error " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Watch (exists) on /test, path: " << response.path << ", type: " << response.type << '\n';
        });

    //event.wait();

    std::cout << "remove\n";

    zk.remove("/test", -1, [&](const RemoveResponse & response)
        {
            if (response.error)
                std::cerr << "Error (remove) " << response.error << ": " << errorMessage(response.error) << '\n';
            else
                std::cerr << "Removed\n";

            //event.set();
        });

    //event.wait();

    std::cout << "multi\n";

    Requests ops;

    {
        CreateRequest create_request;
        create_request.path = "/test";
        create_request.data = "multi1";
        ops.emplace_back(std::make_shared<CreateRequest>(std::move(create_request)));
    }

    {
        SetRequest set_request;
        set_request.path = "/test";
        set_request.data = "multi2";
        ops.emplace_back(std::make_shared<SetRequest>(std::move(set_request)));
    }

    {
        RemoveRequest remove_request;
        remove_request.path = "/test";
        ops.emplace_back(std::make_shared<RemoveRequest>(std::move(remove_request)));
    }

    zk.multi(ops, [&](const MultiResponse & response)
    {
        if (response.error)
            std::cerr << "Error (multi) " << response.error << ": " << errorMessage(response.error) << '\n';
        else
        {
            for (const auto & elem : response.responses)
                if (elem->error)
                    std::cerr << "Error (elem) " << elem->error << ": " << errorMessage(elem->error) << '\n';

            std::cerr << "Created path: " << dynamic_cast<const CreateResponse &>(*response.responses[0]).path_created << '\n';
        }

        event.set();
    });

    event.wait();
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(__PRETTY_FUNCTION__) << '\n';
    return 1;
}
