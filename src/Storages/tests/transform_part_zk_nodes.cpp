#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>

#include <boost/program_options.hpp>

#include <list>
#include <iostream>


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("address,a", boost::program_options::value<std::string>()->required(),
            "addresses of ZooKeeper instances, comma separated. Example: example01e.yandex.ru:2181")
        ("path,p", boost::program_options::value<std::string>()->required(),
            "where to start")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Transform contents of part nodes in ZooKeeper to more compact storage scheme." << std::endl;
        std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    zkutil::ZooKeeper zookeeper(options.at("address").as<std::string>());

    std::string initial_path = options.at("path").as<std::string>();

    struct Node
    {
        Node(
            std::string path_,
            std::future<Coordination::GetResponse> get_future_,
            std::future<Coordination::ListResponse> children_future_,
            Node * parent_)
            : path(std::move(path_))
            , get_future(std::move(get_future_))
            , children_future(std::move(children_future_))
            , parent(parent_)
        {
        }

        std::string path;
        std::future<Coordination::GetResponse> get_future;
        std::future<Coordination::ListResponse> children_future;

        Node * parent = nullptr;
        std::future<Coordination::MultiResponse> set_future;
    };

    std::list<Node> nodes_queue;
    nodes_queue.emplace_back(
        initial_path, zookeeper.asyncGet(initial_path), zookeeper.asyncGetChildren(initial_path), nullptr);

    for (auto it = nodes_queue.begin(); it != nodes_queue.end(); ++it)
    {
        Coordination::GetResponse get_response;
        Coordination::ListResponse children_response;
        try
        {
            get_response = it->get_future.get();
            children_response = it->children_future.get();
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNONODE)
                continue;
            throw;
        }

        if (get_response.stat.ephemeralOwner)
            continue;

        if (it->path.find("/parts/") != std::string::npos
            && !endsWith(it->path, "/columns")
            && !endsWith(it->path, "/checksums"))
        {
            /// The node is related to part.

            /// If it is the part in old format (the node contains children) - convert it to the new format.
            if (!children_response.names.empty())
            {
                auto part_header =  DB::ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
                    zookeeper.get(it->path + "/columns"), zookeeper.get(it->path + "/checksums"));

                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeRemoveRequest(it->path + "/columns", -1));
                ops.emplace_back(zkutil::makeRemoveRequest(it->path + "/checksums", -1));
                ops.emplace_back(zkutil::makeSetRequest(it->path, part_header.toString(), -1));

                it->set_future = zookeeper.asyncMulti(ops);
            }
        }
        else
        {
            /// Recursively add children to the queue.
            for (const auto & name : children_response.names)
            {
                std::string child_path = it->path == "/" ? it->path + name : it->path + '/' + name;
                nodes_queue.emplace_back(
                    child_path, zookeeper.asyncGet(child_path), zookeeper.asyncGetChildren(child_path),
                    &(*it));
            }
        }
    }

    for (auto & node : nodes_queue)
    {
        if (node.set_future.valid())
        {
            node.set_future.get();
            std::cerr << node.path << " changed!" << std::endl;
        }
    }
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
