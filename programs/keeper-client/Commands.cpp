
#include "Commands.h"
#include <queue>
#include "KeeperClient.h"
#include "Parsers/CommonParsers.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool LSCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return true;

    node->args.push_back(std::move(path));
    return true;
}

void LSCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    String path;
    if (!query->args.empty())
        path = client->getAbsolutePath(query->args[0].safeGet<String>());
    else
        path = client->cwd;

    auto children = client->zookeeper->getChildren(path);
    std::sort(children.begin(), children.end());

    bool need_space = false;
    for (const auto & child : children)
    {
        if (std::exchange(need_space, true))
            std::cout << " ";

        std::cout << child;
    }

    std::cout << "\n";
}

bool CDCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return true;

    node->args.push_back(std::move(path));
    return true;
}

void CDCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    if (query->args.empty())
        return;

    auto new_path = client->getAbsolutePath(query->args[0].safeGet<String>());
    if (!client->zookeeper->exists(new_path))
        std::cerr << "Path " << new_path << " does not exist\n";
    else
        client->cwd = new_path;
}

bool SetCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    String arg;
    if (!parseKeeperArg(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    ASTPtr version;
    if (ParserNumber{}.parse(pos, version, expected))
        node->args.push_back(version->as<ASTLiteral &>().value);

    return true;
}

void SetCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    if (query->args.size() == 2)
        client->zookeeper->set(client->getAbsolutePath(query->args[0].safeGet<String>()), query->args[1].safeGet<String>());
    else
        client->zookeeper->set(
            client->getAbsolutePath(query->args[0].safeGet<String>()),
            query->args[1].safeGet<String>(),
            static_cast<Int32>(query->args[2].safeGet<Int32>()));
}

bool CreateCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    String arg;
    if (!parseKeeperArg(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    int mode = zkutil::CreateMode::Persistent;

    if (ParserKeyword(Keyword::PERSISTENT).ignore(pos, expected))
        mode = zkutil::CreateMode::Persistent;
    else if (ParserKeyword(Keyword::EPHEMERAL).ignore(pos, expected))
        mode = zkutil::CreateMode::Ephemeral;
    else if (ParserKeyword(Keyword::EPHEMERAL_SEQUENTIAL).ignore(pos, expected))
        mode = zkutil::CreateMode::EphemeralSequential;
    else if (ParserKeyword(Keyword::PERSISTENT_SEQUENTIAL).ignore(pos, expected))
        mode = zkutil::CreateMode::PersistentSequential;

    node->args.push_back(std::move(mode));

    return true;
}

void CreateCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    client->zookeeper->create(
        client->getAbsolutePath(query->args[0].safeGet<String>()),
        query->args[1].safeGet<String>(),
        static_cast<int>(query->args[2].safeGet<Int64>()));
}

bool TouchCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    return true;
}

void TouchCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    client->zookeeper->createIfNotExists(client->getAbsolutePath(query->args[0].safeGet<String>()), "");
}

bool GetCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    return true;
}

void GetCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    std::cout << client->zookeeper->get(client->getAbsolutePath(query->args[0].safeGet<String>())) << "\n";
}

bool ExistsCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, DB::Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    return true;
}

void ExistsCommand::execute(const DB::ASTKeeperQuery * query, DB::KeeperClient * client) const
{
    std::cout << client->zookeeper->exists(client->getAbsolutePath(query->args[0].safeGet<String>())) << "\n";
}

bool GetStatCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return true;

    node->args.push_back(std::move(path));
    return true;
}

void GetStatCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    Coordination::Stat stat;
    String path;
    if (!query->args.empty())
        path = client->getAbsolutePath(query->args[0].safeGet<String>());
    else
        path = client->cwd;

    client->zookeeper->get(path, &stat);

    std::cout << "cZxid = " << stat.czxid << "\n";
    std::cout << "mZxid = " << stat.mzxid << "\n";
    std::cout << "pZxid = " << stat.pzxid << "\n";
    std::cout << "ctime = " << stat.ctime << "\n";
    std::cout << "mtime = " << stat.mtime << "\n";
    std::cout << "version = " << stat.version << "\n";
    std::cout << "cversion = " << stat.cversion << "\n";
    std::cout << "aversion = " << stat.aversion << "\n";
    std::cout << "ephemeralOwner = " << stat.ephemeralOwner << "\n";
    std::cout << "dataLength = " << stat.dataLength << "\n";
    std::cout << "numChildren = " << stat.numChildren << "\n";
}

namespace
{

/// Helper class for parallelized tree traversal
template <class UserCtx>
struct TraversalTask : public std::enable_shared_from_this<TraversalTask<UserCtx>>
{
    using TraversalTaskPtr = std::shared_ptr<TraversalTask<UserCtx>>;

    struct Ctx
    {
        std::deque<TraversalTaskPtr> new_tasks; /// Tasks for newly discovered children, that hasn't been started yet
        std::deque<std::function<void(Ctx &)>> in_flight_list_requests;  /// In-flight getChildren requests
        std::deque<std::function<void(Ctx &)>> finish_callbacks;    /// Callbacks to be called
        KeeperClient * client;
        UserCtx & user_ctx;

        Ctx(KeeperClient * client_, UserCtx & user_ctx_) : client(client_), user_ctx(user_ctx_) {}
    };

private:
    const fs::path path;
    const TraversalTaskPtr parent;

    Int64 child_tasks = 0;
    Int64 nodes_in_subtree = 1;

public:
    TraversalTask(const fs::path & path_, TraversalTaskPtr parent_)
        : path(path_)
        , parent(parent_)
    {
    }

    /// Start traversing the subtree
    void onStart(Ctx & ctx)
    {
        /// tryGetChildren doesn't throw if the node is not found (was deleted in the meantime)
        std::shared_ptr<std::future<Coordination::ListResponse>> list_request =
            std::make_shared<std::future<Coordination::ListResponse>>(ctx.client->zookeeper->asyncTryGetChildren(path));
        ctx.in_flight_list_requests.push_back([task = this->shared_from_this(), list_request](Ctx & ctx_) mutable
        {
            task->onGetChildren(ctx_, list_request->get());
        });
    }

    /// Called when getChildren request returns
    void onGetChildren(Ctx & ctx, const Coordination::ListResponse & response)
    {
        const bool traverse_children = ctx.user_ctx.onListChildren(path, response.names);

        if (traverse_children)
        {
            /// Schedule traversal of each child
            for (const auto & child : response.names)
            {
                auto task = std::make_shared<TraversalTask>(path / child, this->shared_from_this());
                ctx.new_tasks.push_back(task);
            }
            child_tasks = response.names.size();
        }

        if (child_tasks == 0)
            finish(ctx);
    }

    /// Called when a child subtree has been traversed
    void onChildTraversalFinished(Ctx & ctx, Int64 child_nodes_in_subtree)
    {
        nodes_in_subtree += child_nodes_in_subtree;

        --child_tasks;

        /// Finish if all children have been traversed
        if (child_tasks == 0)
            finish(ctx);
    }

private:
    /// This node and all its children have been traversed
    void finish(Ctx & ctx)
    {
        ctx.user_ctx.onFinishChildrenTraversal(path, nodes_in_subtree);

        if (!parent)
            return;

        /// Notify the parent that we have finished traversing the subtree
        ctx.finish_callbacks.push_back([p = this->parent, child_nodes_in_subtree = this->nodes_in_subtree](Ctx & ctx_)
        {
            p->onChildTraversalFinished(ctx_, child_nodes_in_subtree);
        });
    }
};

/// Traverses the tree in parallel and calls user callbacks
/// Parallelization is achieved by sending multiple async getChildren requests to Keeper, but all processing is done in a single thread
template <class UserCtx>
void parallelized_traverse(const fs::path & path, KeeperClient * client, size_t max_in_flight_requests, UserCtx & ctx_)
{
    typename TraversalTask<UserCtx>::Ctx ctx(client, ctx_);

    auto root_task = std::make_shared<TraversalTask<UserCtx>>(path, nullptr);

    ctx.new_tasks.push_back(root_task);

    /// Until there is something to do
    while (!ctx.new_tasks.empty() || !ctx.in_flight_list_requests.empty() || !ctx.finish_callbacks.empty())
    {
        /// First process all finish callbacks, they don't wait for anything and allow to free memory
        while (!ctx.finish_callbacks.empty())
        {
            auto callback = std::move(ctx.finish_callbacks.front());
            ctx.finish_callbacks.pop_front();
            callback(ctx);
        }

        /// Make new requests if there are less than max in flight
        while (!ctx.new_tasks.empty() && ctx.in_flight_list_requests.size() < max_in_flight_requests)
        {
            auto task = std::move(ctx.new_tasks.front());
            ctx.new_tasks.pop_front();
            task->onStart(ctx);
        }

        /// Wait for first request in the queue to finish
        if (!ctx.in_flight_list_requests.empty())
        {
            auto request = std::move(ctx.in_flight_list_requests.front());
            ctx.in_flight_list_requests.pop_front();
            request(ctx);
        }
    }
}

} /// anonymous namespace

bool FindSuperNodes::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    ASTPtr threshold;
    if (!ParserUnsignedInteger{}.parse(pos, threshold, expected))
        return false;

    node->args.push_back(threshold->as<ASTLiteral &>().value);

    ParserToken{TokenType::Whitespace}.ignore(pos);

    String path;
    if (!parseKeeperPath(pos, expected, path))
        path = ".";

    node->args.push_back(std::move(path));
    return true;
}

void FindSuperNodes::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto threshold = query->args[0].safeGet<UInt64>();
    auto path = client->getAbsolutePath(query->args[1].safeGet<String>());

    struct
    {
        bool onListChildren(const fs::path & path, const Strings & children) const
        {
            if (children.size() >= threshold)
                std::cout << static_cast<String>(path) << "\t" << children.size() << "\n";
            return true;
        }

        void onFinishChildrenTraversal(const fs::path &, Int64) const {}

        size_t threshold;
    } ctx {.threshold = threshold };

    parallelized_traverse(path, client, /* max_in_flight_requests */ 50, ctx);
}

bool DeleteStaleBackups::parse(IParser::Pos & /* pos */, std::shared_ptr<ASTKeeperQuery> & /* node */, Expected & /* expected */) const
{
    return true;
}

void DeleteStaleBackups::execute(const ASTKeeperQuery * /* query */, KeeperClient * client) const
{
    client->askConfirmation(
        "You are going to delete all inactive backups in /clickhouse/backups.",
        [client]
        {
            fs::path backup_root = "/clickhouse/backups";
            auto backups = client->zookeeper->getChildren(backup_root);
            std::sort(backups.begin(), backups.end());

            for (const auto & child : backups)
            {
                auto backup_path = backup_root / child;
                std::cout << "Found backup " << backup_path << ", checking if it's active\n";

                String stage_path = backup_path / "stage";
                auto stages = client->zookeeper->getChildren(stage_path);

                bool is_active = false;
                for (const auto & stage : stages)
                {
                    if (startsWith(stage, "alive"))
                    {
                        is_active = true;
                        break;
                    }
                }

                if (is_active)
                {
                    std::cout << "Backup " << backup_path << " is active, not going to delete\n";
                    continue;
                }

                std::cout << "Backup " << backup_path << " is not active, deleting it\n";
                client->zookeeper->removeRecursive(backup_path);
            }
        });
}

bool FindBigFamily::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        path = ".";

    node->args.push_back(std::move(path));

    ASTPtr count;
    if (ParserUnsignedInteger{}.parse(pos, count, expected))
        node->args.push_back(count->as<ASTLiteral &>().value);
    else
        node->args.push_back(UInt64(10));

    return true;
}

void FindBigFamily::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto path = client->getAbsolutePath(query->args[0].safeGet<String>());
    auto n = query->args[1].safeGet<UInt64>();

    struct
    {
        std::vector<std::tuple<Int64, String>> result;

        bool onListChildren(const fs::path &, const Strings &) const { return true; }

        void onFinishChildrenTraversal(const fs::path & path, Int64 nodes_in_subtree)
        {
            result.emplace_back(nodes_in_subtree, path.string());
        }
    } ctx;

    parallelized_traverse(path, client, /* max_in_flight_requests */ 50, ctx);

    std::sort(ctx.result.begin(), ctx.result.end(), std::greater());
    for (UInt64 i = 0; i < std::min(ctx.result.size(), static_cast<size_t>(n)); ++i)
        std::cout << std::get<1>(ctx.result[i]) << "\t" << std::get<0>(ctx.result[i]) << "\n";
}

bool RMCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    ASTPtr version;
    if (ParserNumber{}.parse(pos, version, expected))
        node->args.push_back(version->as<ASTLiteral &>().value);

    return true;
}

void RMCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    Int32 version{-1};
    if (query->args.size() == 2)
        version = static_cast<Int32>(query->args[1].safeGet<Int32>());

    client->zookeeper->remove(client->getAbsolutePath(query->args[0].safeGet<String>()), version);
}

bool RMRCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    ASTPtr remove_nodes_limit;
    if (ParserUnsignedInteger{}.parse(pos, remove_nodes_limit, expected))
        node->args.push_back(remove_nodes_limit->as<ASTLiteral &>().value);
    else
        node->args.push_back(UInt64(100));

    return true;
}

void RMRCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    String path = client->getAbsolutePath(query->args[0].safeGet<String>());
    UInt64 remove_nodes_limit = query->args[1].safeGet<UInt64>();

    client->askConfirmation(
        "You are going to recursively delete path " + path,
        [client, path, remove_nodes_limit] { client->zookeeper->removeRecursive(path, static_cast<UInt32>(remove_nodes_limit)); });
}

bool ReconfigCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, DB::Expected & expected) const
{
    ParserKeyword s_add(Keyword::ADD);
    ParserKeyword s_remove(Keyword::REMOVE);
    ParserKeyword s_set(Keyword::SET);

    ReconfigCommand::Operation operation;
    if (s_add.ignore(pos, expected))
        operation = ReconfigCommand::Operation::ADD;
    else if (s_remove.ignore(pos, expected))
        operation = ReconfigCommand::Operation::REMOVE;
    else if (s_set.ignore(pos, expected))
        operation = ReconfigCommand::Operation::SET;
    else
        return false;

    node->args.push_back(operation);
    ParserToken{TokenType::Whitespace}.ignore(pos);

    String arg;
    if (!parseKeeperArg(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    return true;
}

void ReconfigCommand::execute(const DB::ASTKeeperQuery * query, DB::KeeperClient * client) const
{
    String joining;
    String leaving;
    String new_members;

    auto operation = query->args[0].safeGet<ReconfigCommand::Operation>();
    switch (operation)
    {
        case static_cast<UInt8>(ReconfigCommand::Operation::ADD):
            joining = query->args[1].safeGet<String>();
            break;
        case static_cast<UInt8>(ReconfigCommand::Operation::REMOVE):
            leaving = query->args[1].safeGet<String>();
            break;
        case static_cast<UInt8>(ReconfigCommand::Operation::SET):
            new_members = query->args[1].safeGet<String>();
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected operation: {}", operation);
    }

    auto response = client->zookeeper->reconfig(joining, leaving, new_members);
    std::cout << response.value << '\n';
}

bool SyncCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, DB::Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    return true;
}

void SyncCommand::execute(const DB::ASTKeeperQuery * query, DB::KeeperClient * client) const
{
    std::cout << client->zookeeper->sync(client->getAbsolutePath(query->args[0].safeGet<String>())) << "\n";
}

bool HelpCommand::parse(IParser::Pos & /* pos */, std::shared_ptr<ASTKeeperQuery> & /* node */, Expected & /* expected */) const
{
    return true;
}

void HelpCommand::execute(const ASTKeeperQuery * /* query */, KeeperClient * /* client */) const
{
    for (const auto & pair : KeeperClient::commands)
        std::cout << pair.second->generateHelpString() << "\n";
}

bool FourLetterWordCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    expected.add(pos, "four-letter-word command");
    if (pos->type != TokenType::BareWord)
        return false;

    String cmd(pos->begin, pos->end);
    if (cmd.size() != 4)
        return false;

    ++pos;
    node->args.push_back(std::move(cmd));
    return true;
}

void FourLetterWordCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    std::cout << client->executeFourLetterCommand(query->args[0].safeGet<String>()) << "\n";
}

bool GetDirectChildrenNumberCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        path = ".";

    node->args.push_back(std::move(path));

    return true;
}

void GetDirectChildrenNumberCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto path = client->getAbsolutePath(query->args[0].safeGet<String>());

    Coordination::Stat stat;
    client->zookeeper->get(path, &stat);

    std::cout << stat.numChildren << "\n";
}

bool GetAllChildrenNumberCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        path = ".";

    node->args.push_back(std::move(path));

    return true;
}

void GetAllChildrenNumberCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto path = client->getAbsolutePath(query->args[0].safeGet<String>());

    std::queue<fs::path> queue;
    queue.push(path);
    Coordination::Stat stat;
    client->zookeeper->get(path, &stat);

    int totalNumChildren = stat.numChildren;
    while (!queue.empty())
    {
        auto next_path = queue.front();
        queue.pop();

        auto children = client->zookeeper->getChildren(next_path);
        for (auto & child : children)
            child = next_path / child;
        auto response = client->zookeeper->get(children);

        for (size_t i = 0; i < response.size(); ++i)
        {
            totalNumChildren += response[i].stat.numChildren;
            queue.push(children[i]);
        }
    }

    std::cout << totalNumChildren << "\n";
}

namespace
{

class CPMVOperation
{
    constexpr static UInt64 kTryLimit = 1000;

public:
    CPMVOperation(String src_, String dest_, bool remove_src_, KeeperClient * client_)
        : src(std::move(src_)), dest(std::move(dest_)), remove_src(remove_src_), client(client_)
    {
    }

    bool isTryLimitReached() const { return failed_tries_count >= kTryLimit; }

    bool isCompleted() const { return is_completed; }

    void perform()
    {
        Coordination::Stat src_stat;
        String data = client->zookeeper->get(src, &src_stat);

        Coordination::Requests ops{
            zkutil::makeCheckRequest(src, src_stat.version),
            zkutil::makeCreateRequest(dest, data, zkutil::CreateMode::Persistent), // Do we need to copy ACLs here?
        };

        if (remove_src)
            ops.push_back(zkutil::makeRemoveRequest(src, src_stat.version));

        Coordination::Responses responses;
        auto code = client->zookeeper->tryMulti(ops, responses);

        switch (code)
        {
            case Coordination::Error::ZOK: {
                is_completed = true;
                return;
            }
            case Coordination::Error::ZBADVERSION: {
                ++failed_tries_count;

                if (isTryLimitReached())
                    zkutil::KeeperMultiException::check(code, ops, responses);

                return;
            }
            default:
                zkutil::KeeperMultiException::check(code, ops, responses);
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable");
    }

private:
    String src;
    String dest;
    bool remove_src = false;
    KeeperClient * client = nullptr;

    bool is_completed = false;
    uint64_t failed_tries_count = 0;
};

}

bool CPCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, [[maybe_unused]] Expected & expected) const
{
    String src_path;
    if (!parseKeeperPath(pos, expected, src_path))
        return false;
    node->args.push_back(std::move(src_path));

    String to_path;
    if (!parseKeeperPath(pos, expected, to_path))
        return false;
    node->args.push_back(std::move(to_path));

    return true;
}

void CPCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto src = client->getAbsolutePath(query->args[0].safeGet<String>());
    auto dest = client->getAbsolutePath(query->args[1].safeGet<String>());

    CPMVOperation operation(std::move(src), std::move(dest), /*remove_src_=*/false, /*client_=*/client);

    while (!operation.isTryLimitReached() && !operation.isCompleted())
        operation.perform();
}

bool MVCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String src_path;
    if (!parseKeeperPath(pos, expected, src_path))
        return false;
    node->args.push_back(std::move(src_path));

    String to_path;
    if (!parseKeeperPath(pos, expected, to_path))
        return false;
    node->args.push_back(std::move(to_path));

    return true;
}

void MVCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    auto src = client->getAbsolutePath(query->args[0].safeGet<String>());
    auto dest = client->getAbsolutePath(query->args[1].safeGet<String>());

    CPMVOperation operation(std::move(src), std::move(dest), /*remove_src_=*/true, /*client_=*/client);

    while (!operation.isTryLimitReached() && !operation.isCompleted())
        operation.perform();
}

}
