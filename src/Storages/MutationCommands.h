#pragma once

#include <optional>
#include <vector>
#include <memory>
#include <unordered_map>

#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTAlterCommand;
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
        MATERIALIZE_PROJECTION,
        MATERIALIZE_STATISTICS,
        READ_COLUMN, /// Read column and apply conversions (MODIFY COLUMN alter query).
        DROP_COLUMN,
        DROP_INDEX,
        DROP_PROJECTION,
        DROP_STATISTICS,
        MATERIALIZE_TTL,
        REWRITE_PARTS,
        RENAME_COLUMN,
        MATERIALIZE_COLUMN,
        APPLY_DELETED_MASK,
        APPLY_PATCHES,
        ALTER_WITHOUT_MUTATION, /// pure metadata command
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

    /// A version of mutation to which command corresponds.
    std::optional<UInt64> mutation_version = {};

    /// True if column is read by mutation command to apply patch.
    /// Required to distinguish read command used for MODIFY COLUMN.
    bool read_for_patch = false;

    /// If parse_alter_commands, than consider more Alter commands as mutation commands
    static std::optional<MutationCommand> parse(ASTAlterCommand * command, bool parse_alter_commands = false, bool with_pure_metadata_commands = false);

    /// This command shouldn't stick with other commands
    bool isBarrierCommand() const;
    bool isPureMetadataCommand() const;
    bool isEmptyCommand() const;
    bool isDropOrRename() const;
    bool affectsAllColumns() const;
};

/// Multiple mutation commands, possibly from different ALTER queries
class MutationCommands : public std::vector<MutationCommand>
{
public:
    std::shared_ptr<ASTExpressionList> ast(bool with_pure_metadata_commands = false) const;

    void writeText(WriteBuffer & out, bool with_pure_metadata_commands) const;
    void readText(ReadBuffer & in, bool with_pure_metadata_commands);
    std::string toString(bool with_pure_metadata_commands) const;
    bool hasNonEmptyMutationCommands() const;

    bool hasAnyUpdateCommand() const;
    bool hasOnlyUpdateCommands() const;

    /// These set of commands contain barrier command and shouldn't
    /// stick with other commands. Commands from one set have already been validated
    /// to be executed without issues on the creation state.
    bool containBarrierCommand() const;
    NameSet getAllUpdatedColumns() const;
};

using MutationCommandsConstPtr = std::shared_ptr<MutationCommands>;

/// A pair of Actions DAG that is required to execute one step
/// of mutation and the name of filter column if it's a filtering step.
struct MutationActions
{
    ActionsDAG dag;
    String filter_column_name;
    bool project_input;
    std::optional<UInt64> mutation_version;
};

}
