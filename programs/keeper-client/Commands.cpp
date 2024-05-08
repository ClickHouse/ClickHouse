
#include "Commands.h"
#include <queue>
#include "KeeperClient.h"
#include "Parsers/CommonParsers.h"


namespace DB
{

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
        std::cerr << "Path " << new_path << " does not exists\n";
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
            static_cast<Int32>(query->args[2].get<Int32>()));
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

bool FindSuperNodes::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    ASTPtr threshold;
    if (!ParserUnsignedInteger{}.parse(pos, threshold, expected))
        return false;

    node->args.push_back(threshold->as<ASTLiteral &>().value);

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

    Coordination::Stat stat;
    client->zookeeper->get(path, &stat);

    if (stat.numChildren >= static_cast<Int32>(threshold))
    {
        std::cout << static_cast<String>(path) << "\t" << stat.numChildren << "\n";
        return;
    }

    auto children = client->zookeeper->getChildren(path);
    std::sort(children.begin(), children.end());
    for (const auto & child : children)
    {
        auto next_query = *query;
        next_query.args[1] = DB::Field(path / child);
        execute(&next_query, client);
    }
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

    std::vector<std::tuple<Int32, String>> result;

    std::queue<fs::path> queue;
    queue.push(path);
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
            result.emplace_back(response[i].stat.numChildren, children[i]);
            queue.push(children[i]);
        }
    }

    std::sort(result.begin(), result.end(), std::greater());
    for (UInt64 i = 0; i < std::min(result.size(), static_cast<size_t>(n)); ++i)
        std::cout << std::get<1>(result[i]) << "\t" << std::get<0>(result[i]) << "\n";
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
        version = static_cast<Int32>(query->args[1].get<Int32>());

    client->zookeeper->remove(client->getAbsolutePath(query->args[0].safeGet<String>()), version);
}

bool RMRCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String path;
    if (!parseKeeperPath(pos, expected, path))
        return false;
    node->args.push_back(std::move(path));

    return true;
}

void RMRCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    String path = client->getAbsolutePath(query->args[0].safeGet<String>());
    client->askConfirmation(
        "You are going to recursively delete path " + path, [client, path] { client->zookeeper->removeRecursive(path); });
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

    auto operation = query->args[0].get<ReconfigCommand::Operation>();
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
            UNREACHABLE();
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

}
