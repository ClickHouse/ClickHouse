#pragma once

#include "Parser.h"

namespace DB
{

class KeeperClient;

class IKeeperClientCommand
{
public:
    static const String name;

    virtual bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const = 0;

    virtual void execute(const ASTKeeperQuery * query, KeeperClient * client) const = 0;

    virtual String getHelpMessage() const = 0;

    virtual String getName() const = 0;

    virtual ~IKeeperClientCommand() = default;
};

using Command = std::shared_ptr<IKeeperClientCommand>;


class LSCommand : public IKeeperClientCommand
{
    String getName() const override { return "ls"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "ls [path] -- Lists the nodes for the given path (default: cwd)"; }
};

class CDCommand : public IKeeperClientCommand
{
    String getName() const override { return "cd"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "cd [path] -- Change the working path (default `.`)"; }
};

class SetCommand : public IKeeperClientCommand
{
    String getName() const override { return "set"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "set <path> <value> [version] -- Updates the node's value. Only update if version matches (default: -1)";
    }
};

class CreateCommand : public IKeeperClientCommand
{
    String getName() const override { return "create"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "create <path> <value> -- Creates new node"; }
};

class GetCommand : public IKeeperClientCommand
{
    String getName() const override { return "get"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "get <path> -- Returns the node's value"; }
};

class RMCommand : public IKeeperClientCommand
{
    String getName() const override { return "rm"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "remove <path> -- Remove the node"; }
};

class RMRCommand : public IKeeperClientCommand
{
    String getName() const override { return "rmr"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "rmr <path> -- Recursively deletes path. Confirmation required"; }
};

class HelpCommand : public IKeeperClientCommand
{
    String getName() const override { return "help"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "help -- Prints this message"; }
};

class FourLetterWordCommand : public IKeeperClientCommand
{
    String getName() const override { return "flwc"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "flwc <command> -- Executes four-letter-word command"; }
};

}
