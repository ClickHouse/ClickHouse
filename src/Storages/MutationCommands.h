#pragma once

#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage_fwd.h>
#include <DataTypes/IDataType.h>
#include <Core/Names.h>

#include <optional>
#include <unordered_map>


namespace DB
{

class Context;
class WriteBuffer;
class ReadBuffer;

/// Represents set of actions which should be applied
/// to values from set of columns which satisfy predicate.
struct MutationCommand
{
    ASTPtr ast; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
        UPDATE,
        MATERIALIZE_INDEX,
        READ_COLUMN, /// Read column and apply conversions (MODIFY COLUMN alter query).
        DROP_COLUMN,
        DROP_INDEX,
        MATERIALIZE_TTL,
        RENAME_COLUMN,
    };

    Type type = EMPTY;

    /// WHERE part of mutation
    ASTPtr predicate;

    /// Columns with corresponding actions
    std::unordered_map<String, ASTPtr> column_to_update_expression;

    /// For MATERIALIZE INDEX.
    String index_name;

    /// For MATERIALIZE INDEX, UPDATE and DELETE.
    ASTPtr partition;

    /// For reads, drops and etc.
    String column_name;
    DataTypePtr data_type; /// Maybe empty if we just want to drop column

    /// We need just clear column, not drop from metadata.
    bool clear = false;

    /// Column rename_to
    String rename_to;

    /// If parse_alter_commands, than consider more Alter commands as mutation commands
    static std::optional<MutationCommand> parse(ASTAlterCommand * command, bool parse_alter_commands = false);
};

/// Multiple mutation commands, possible from different ALTER queries
class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTExpressionList> ast() const;

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
};

}
