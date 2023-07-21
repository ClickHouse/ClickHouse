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

    String generateHelpString() const
    {
        return fmt::vformat(getHelpMessage(), fmt::make_format_args(getName()));
    }

};

using Command = std::shared_ptr<IKeeperClientCommand>;


class LSCommand : public IKeeperClientCommand
{
    String getName() const override { return "ls"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} [path] -- Lists the nodes for the given path (default: cwd)"; }
};

class CDCommand : public IKeeperClientCommand
{
    String getName() const override { return "cd"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} [path] -- Change the working path (default `.`)"; }
};

class SetCommand : public IKeeperClientCommand
{
    String getName() const override { return "set"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <path> <value> [version] -- Updates the node's value. Only update if version matches (default: -1)";
    }
};

class CreateCommand : public IKeeperClientCommand
{
    String getName() const override { return "create"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> <value> -- Creates new node"; }
};

class GetCommand : public IKeeperClientCommand
{
    String getName() const override { return "get"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Returns the node's value"; }
};

class GetStatCommand : public IKeeperClientCommand
{
    String getName() const override { return "get_stat"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} [path] -- Returns the node's stat (default `.`)"; }
};

class FindSupperNodes : public IKeeperClientCommand
{
    String getName() const override { return "find_super_nodes"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <threshold> [path] -- Finds nodes with number of children larger than some threshold for the given path (default `.`)";
    }
};

class DeleteStableBackups : public IKeeperClientCommand
{
    String getName() const override { return "delete_stable_backups"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} -- Deletes ClickHouse nodes used for backups that are now inactive";
    }
};

class FindBigFamily : public IKeeperClientCommand
{
    String getName() const override { return "find_big_family"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} [path] [n] -- Returns the top n nodes with the biggest family in the subtree (default path = `.` and n = 10)";
    }
};


class RMCommand : public IKeeperClientCommand
{
    String getName() const override { return "rm"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Remove the node"; }
};

class RMRCommand : public IKeeperClientCommand
{
    String getName() const override { return "rmr"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Recursively deletes path. Confirmation required"; }
};

class HelpCommand : public IKeeperClientCommand
{
    String getName() const override { return "help"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} -- Prints this message"; }
};

class FourLetterWordCommand : public IKeeperClientCommand
{
    String getName() const override { return "flwc"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <command> -- Executes four-letter-word command"; }
};

}
