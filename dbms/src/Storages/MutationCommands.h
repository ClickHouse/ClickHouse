#pragma once

#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage_fwd.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <unordered_map>


namespace DB
{

class Context;
class WriteBuffer;
class ReadBuffer;

/// Represents set of actions which should be applied
/// to values from set of columns which statisfy predicate.
struct MutationCommand
{
    ASTPtr ast; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
        UPDATE,
        MATERIALIZE_INDEX,
        READ_COLUMN,
        DROP_COLUMN,
        DROP_INDEX,
    };

    Type type = EMPTY;

    /// WHERE part of mutation
    ASTPtr predicate;

    /// Columns with corresponding actions
    std::unordered_map<String, ASTPtr> column_to_update_expression;

    /// For MATERIALIZE INDEX
    String index_name;
    ASTPtr partition;

    /// For reads, drops and etc.
    String column_name;
    DataTypePtr data_type;

    /// If from_zookeeper, than consider more Alter commands as mutation commands
    static std::optional<MutationCommand> parse(ASTAlterCommand * command, bool from_zookeeper=false);
};

/// Multiple mutation commands, possible from different ALTER queries
class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTAlterCommandList> ast() const;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
