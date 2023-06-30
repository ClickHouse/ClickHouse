
#include "Commands.h"
#include "KeeperClient.h"


namespace DB
{

bool LSCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return true;

    node->args.push_back(std::move(arg));
    return true;
}

void LSCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    String path;
    if (!query->args.empty())
        path = client->getAbsolutePath(query->args[0].safeGet<String>());
    else
        path = client->cwd;

    for (const auto & child : client->zookeeper->getChildren(path))
        std::cout << child << " ";
    std::cout << "\n";
}

bool CDCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return true;

    node->args.push_back(std::move(arg));
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
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

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
            static_cast<Int32>(query->args[2].safeGet<Int64>()));
}

bool CreateCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    if (!parseKeeperArg(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    int mode = zkutil::CreateMode::Persistent;

    if (ParserKeyword{"PERSISTENT"}.ignore(pos, expected))
        mode = zkutil::CreateMode::Persistent;
    else if (ParserKeyword{"EPHEMERAL"}.ignore(pos, expected))
        mode = zkutil::CreateMode::Ephemeral;
    else if (ParserKeyword{"EPHEMERAL SEQUENTIAL"}.ignore(pos, expected))
        mode = zkutil::CreateMode::EphemeralSequential;
    else if (ParserKeyword{"PERSISTENT SEQUENTIAL"}.ignore(pos, expected))
        mode = zkutil::CreateMode::PersistentSequential;

    node->args.push_back(mode);

    return true;
}

void CreateCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    client->zookeeper->create(
        client->getAbsolutePath(query->args[0].safeGet<String>()),
        query->args[1].safeGet<String>(),
        static_cast<int>(query->args[2].safeGet<Int64>()));
}

bool GetCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    return true;
}

void GetCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    std::cout << client->zookeeper->get(client->getAbsolutePath(query->args[0].safeGet<String>())) << "\n";
}

bool RMCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    return true;
}

void RMCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    client->zookeeper->remove(client->getAbsolutePath(query->args[0].safeGet<String>()));
}

bool RMRCommand::parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const
{
    String arg;
    if (!parseKeeperPath(pos, expected, arg))
        return false;
    node->args.push_back(std::move(arg));

    return true;
}

void RMRCommand::execute(const ASTKeeperQuery * query, KeeperClient * client) const
{
    String path = client->getAbsolutePath(query->args[0].safeGet<String>());
    client->askConfirmation("You are going to recursively delete path " + path,
                            [client, path]{ client->zookeeper->removeRecursive(path); });
}

bool HelpCommand::parse(IParser::Pos & /* pos */, std::shared_ptr<ASTKeeperQuery> & /* node */, Expected & /* expected */) const
{
    return true;
}

void HelpCommand::execute(const ASTKeeperQuery * /* query */, KeeperClient * /* client */) const
{
    for (const auto & pair : KeeperClient::commands)
        std::cout << pair.second->getHelpMessage() << "\n";
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

}
