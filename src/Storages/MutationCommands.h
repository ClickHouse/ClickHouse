#pragma once

#include <optional>
#include <vector>
#include <memory>
#include <unordered_map>

#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage_fwd.h>
#include <DataTypes/IDataType.h>
#include <Core/Names.h>

namespace DB
{

class Context;
class WriteBuffer;
class ReadBuffer;

/// Represents set of actions which should be applied
/// to values from set of columns which satisfy predicate.
struct MutationCommand
{
    ASTPtr ast = {}; /// The AST of the whole command

    enum Type
    {
        EMPTY,     /// Not used.
        DELETE,
        UPDATE,
        MATERIALIZE_INDEX,
        MATERIALIZE_INDEXES,
        MATERIALIZE_PROJECTION,
        MATERIALIZE_PROJECTIONS,
        MATERIALIZE_STATISTICS,
        READ_COLUMN, /// Read column and apply conversions (MODIFY COLUMN alter query).
        DROP_COLUMN,
        DROP_INDEX,
        DROP_PROJECTION,
        DROP_STATISTICS,
        MATERIALIZE_TTL,
        RENAME_COLUMN,
        MATERIALIZE_COLUMN,
        MATERIALIZE_COLUMNS,
        APPLY_DELETED_MASK,
        ALTER_WITHOUT_MUTATION, /// pure metadata command, currently unusned
    };

    Type type = EMPTY;

    /// WHERE part of mutation
    ASTPtr predicate = {};

    /// Columns with corresponding actions
    std::unordered_map<String, ASTPtr> column_to_update_expression = {};

    /// For MATERIALIZE INDEX and PROJECTION and STATISTICS
    String index_name = {};
    String projection_name = {};
    std::vector<String> statistics_columns = {};
    std::vector<String> statistics_types = {};

    /// For MATERIALIZE INDEX, UPDATE and DELETE.
    ASTPtr partition = {};

    /// For reads, drops and etc.
    String column_name = {};
    DataTypePtr data_type = {}; /// Maybe empty if we just want to drop column

    /// We need just clear column, not drop from metadata.
    bool clear = false;

    /// Column rename_to
    String rename_to = {};

    /// If parse_alter_commands, than consider more Alter commands as mutation commands
    static std::optional<MutationCommand> parse(ASTAlterCommand * command, bool parse_alter_commands = false);

    /// This command shouldn't stick with other commands
    bool isBarrierCommand() const;
};

/// Multiple mutation commands, possible from different ALTER queries
class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTExpressionList> ast(bool with_pure_metadata_commands = false) const;

    void writeText(WriteBuffer & out, bool with_pure_metadata_commands) const;
    void readText(ReadBuffer & in);
    std::string toString() const;
    bool hasNonEmptyMutationCommands() const;

    /// These set of commands contain barrier command and shouldn't
    /// stick with other commands. Commands from one set have already been validated
    /// to be executed without issues on the creation state.
    bool containBarrierCommand() const;
};

using MutationCommandsConstPtr = std::shared_ptr<MutationCommands>;

}
