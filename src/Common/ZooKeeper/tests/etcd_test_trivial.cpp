//#include <gtest/gtest.h>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <iostream>
#include <future>

void createAndWait(
    Coordination::EtcdKeeper & etcdkeeper,
    const std::string & path,
    const std::string & data,
    bool is_ephemeral,
    bool is_sequential)
{
    std::cout << "Will create path: " << path << std::endl;

    std::promise<std::string> create_promise;
    std::future<std::string> create_future = create_promise.get_future();

    etcdkeeper.create(path, data, is_ephemeral, is_sequential, {},
      [&](const Coordination::CreateResponse & response)
      {
          if (response.error != Coordination::Error::ZOK)
              std::cerr << "Error: " << Coordination::errorMessage(response.error) << "\n";
          else
          {
              std::cerr << "Path created: " << response.path_created << "\n";
          }
          create_promise.set_value("Create callback called.");
      });

    std::cout << create_future.get() << std::endl;
}

const std::vector<std::string> paths =
    {"/home", "home/jakalletti", "home/jakalletti/ClickHouse",
        "home/jakalletti/ClickHouse/build", "home/jakalletti/ClickHouse/build/debug"};

void simpleCreatePaths(Coordination::EtcdKeeper & etcdkeeper)
{
    for (auto & path: paths)
    {
        createAndWait(etcdkeeper, path, path, false, false);
    }

    std::cout << "SimpleCreatePaths Done" << std::endl;
}


void simpleCreateSequentialPaths(Coordination::EtcdKeeper & etcdkeeper)
{
    for (auto & path: paths)
    {
        for (int i = 0; i < 10; ++i)
            createAndWait(etcdkeeper, path, path, true, true);
    }
    std::cout << "simpleCreateSequentialPaths Done" << std::endl;
}




int main()
{
    Coordination::EtcdKeeper etcdkeeper
        ("",
         "localhost:2379",
         Poco::Timespan(0,  1000));

    std::cout << "EtcdKeeper created:" << std::endl;

    simpleCreatePaths(etcdkeeper);
    simpleCreateSequentialPaths(etcdkeeper);

    std::promise<std::string> create_promise;
    std::future<std::string> create_future = create_promise.get_future();

    etcdkeeper.create("/test", "hello", false, false, {},
      [&](const Coordination::CreateResponse & response)
      {
        if (response.error != Coordination::Error::ZOK)
            std::cerr << "Error: " << Coordination::errorMessage(response.error) << "\n";
        else
        {
            std::cerr << "Path created: " << response.path_created << "\n";
        }
        create_promise.set_value("Create callback called.");
      });

    std::cout << create_future.get() << std::endl;

    std::promise<std::string> exists_promise;
    std::future<std::string> exists_future = exists_promise.get_future();

    std::promise<std::string> watch_promise;
    std::future<std::string> watch_future = watch_promise.get_future();

    etcdkeeper.exists("/test",
      [&](const Coordination::ExistsResponse & exists_response)
      {
        std::cout << exists_response.stat.dump() << std::endl;
        exists_promise.set_value("Exists callback called.");
      },
      [&](const Coordination::WatchResponse & watch_response)
      {
        std::cout << watch_response.dump();
        watch_promise.set_value("Watch callback called.");
      }
    );

    std::cout << exists_future.get() << std::endl;

    std::promise<std::string> new_create_promise;
    std::future<std::string> new_create_future = new_create_promise.get_future();

    etcdkeeper.create("/test/node", "hello2", false, false, {},
        [&](const Coordination::CreateResponse & response)
        {
          if (response.error != Coordination::Error::ZOK)
              std::cerr << "Error: " << Coordination::errorMessage(response.error) << "\n";
          else
          {
              std::cerr << "Path created: " << response.path_created << "\n";
          }
          create_promise.set_value("Create callback called.");
        });

    std::cout << new_create_future.get() << std::endl;
    std::cout << watch_future.get() << std::endl;

    _exit(0);
}
