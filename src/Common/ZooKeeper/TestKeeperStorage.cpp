#include <Common/ZooKeeper/TestKeeperStorage.h>
#include <Common/ZooKeeper/IKeeper.h>

namespace zkutil
{
using namespace DB;

static String parentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}

using Undo = std::function<void()>;

struct TestKeeperStorageRequest
{
    Coordination::ZooKeeperRequestPtr zk_request;

    TestKeeperStorageRequest(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : zk_request(zk_request_)
    {}
    virtual bool isMutable() const { return false; }
    virtual std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const = 0;
    virtual ~TestKeeperStorageRequest() {}
};


struct TestKeeperStorageCreateRequest final : public TestKeeperStorageRequest
{
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Undo undo;
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        if (container.count(request.path))
        {
            response.error = Coordination::Error::ZNODEEXISTS;
        }
        else
        {
            auto it = container.find(parentPath(request.path));

            if (it == container.end())
            {
                response.error = Coordination::Error::ZNONODE;
            }
            else if (it->second.is_ephemeral)
            {
                response.error = Coordination::Error::ZNOCHILDRENFOREPHEMERALS;
            }
            else
            {
                TestKeeperStorage::Node created_node;
                created_node.seq_num = 0;
                created_node.stat.czxid = zxid;
                created_node.stat.mzxid = zxid;
                created_node.stat.ctime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
                created_node.stat.mtime = created_node.stat.ctime;
                created_node.stat.numChildren = 0;
                created_node.stat.dataLength = request.data.length();
                created_node.data = request.data;
                created_node.is_ephemeral = request.is_ephemeral;
                created_node.is_sequental = request.is_sequential;
                std::string path_created = request.path;

                if (request.is_sequential)
                {
                    auto seq_num = it->second.seq_num;
                    ++it->second.seq_num;

                    std::stringstream seq_num_str;
                    seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

                    path_created += seq_num_str.str();
                }

                response.path_created = path_created;
                container.emplace(path_created, std::move(created_node));

                undo = [&container, path_created, is_sequential = request.is_sequential, parent_path = it->first]
                {
                    container.erase(path_created);
                    auto & undo_parent = container.at(parent_path);
                    --undo_parent.stat.cversion;
                    --undo_parent.stat.numChildren;

                    if (is_sequential)
                        --undo_parent.seq_num;
                };

                ++it->second.stat.cversion;
                ++it->second.stat.numChildren;

                response.error = Coordination::Error::ZOK;
            }
        }

        return { response_ptr, undo };
    }
};


 
}
