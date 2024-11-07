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

    String getHelpMessage() const override { return "{} [path] -- Changes the working path (default `.`)"; }
};

class SetCommand : public IKeeperClientCommand
{
    String getName() const override { return "set"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <path> <value> [version] -- Updates the node's value. Only updates if version matches (default: -1)";
    }
};

class CreateCommand : public IKeeperClientCommand
{
    String getName() const override { return "create"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> <value> [mode] -- Creates new node with the set value"; }
};

class TouchCommand : public IKeeperClientCommand
{
    String getName() const override { return "touch"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Creates new node with an empty string as value. Doesn't throw an exception if the node already exists"; }
};

class GetCommand : public IKeeperClientCommand
{
    String getName() const override { return "get"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Returns the node's value"; }
};

class ExistsCommand : public IKeeperClientCommand
{
    String getName() const override { return "exists"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Returns `1` if node exists, `0` otherwise"; }
};

class GetStatCommand : public IKeeperClientCommand
{
    String getName() const override { return "get_stat"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} [path] -- Returns the node's stat (default `.`)"; }
};

class FindSuperNodes : public IKeeperClientCommand
{
    String getName() const override { return "find_super_nodes"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <threshold> [path] -- Finds nodes with number of children larger than some threshold for the given path (default `.`)";
    }
};

class DeleteStaleBackups : public IKeeperClientCommand
{
    String getName() const override { return "delete_stale_backups"; }

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

    String getHelpMessage() const override { return "{} <path> [version] -- Removes the node only if version matches (default: -1)"; }
};

class RMRCommand : public IKeeperClientCommand
{
    String getName() const override { return "rmr"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> [limit] -- Recursively deletes path if the subtree size is smaller than the limit. Confirmation required (default limit = 100)"; }
};

class ReconfigCommand : public IKeeperClientCommand
{
    enum class Operation : UInt8
    {
        ADD = 0,
        REMOVE = 1,
        SET = 2,
    };

    String getName() const override { return "reconfig"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <add|remove|set> \"<arg>\" [version] -- Reconfigure Keeper cluster. See https://clickhouse.com/docs/en/guides/sre/keeper/clickhouse-keeper#reconfiguration"; }
};

class SyncCommand: public IKeeperClientCommand
{
    String getName() const override { return "sync"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override { return "{} <path> -- Synchronizes node between processes and leader"; }
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

class GetDirectChildrenNumberCommand : public IKeeperClientCommand
{
    String getName() const override { return "get_direct_children_number"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} [path] -- Get numbers of direct children nodes under a specific path";
    }
};

class GetAllChildrenNumberCommand : public IKeeperClientCommand
{
    String getName() const override { return "get_all_children_number"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} [path] -- Get all numbers of children nodes under a specific path";
    }
};

class CPCommand : public IKeeperClientCommand
{
    String getName() const override { return "cp"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <src> <dest> -- Copies 'src' node to 'dest' path.";
    }
};

class MVCommand : public IKeeperClientCommand
{
    String getName() const override { return "mv"; }

    bool parse(IParser::Pos & pos, std::shared_ptr<ASTKeeperQuery> & node, Expected & expected) const override;

    void execute(const ASTKeeperQuery * query, KeeperClient * client) const override;

    String getHelpMessage() const override
    {
        return "{} <src> <dest> -- Moves 'src' node to the 'dest' path.";
    }
};

}
