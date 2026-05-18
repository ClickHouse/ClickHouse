#pragma once

#include <optional>
#include <vector>
#include <memory>
#include <unordered_map>

#include <Core/Defines.h>
#include <Core/Names.h>
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
    /// Serialized text of the whole command (output of `formatWithSecretsOneLine`).
    /// The AST itself is not kept around - call `accessAst` to parse it on demand.
    String ast_text = {};

    /// Parser limits captured at the time `ast_text` was produced. Used by
    /// `accessAst` so that on-demand re-parsing uses the same limits as the
    /// original successful parse. They are stored alongside the text instead
    /// of caching the parsed AST - the AST is always parsed fresh on demand.
    UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;
    UInt64 max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;

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

    /// RAII handle returned by `accessAst`.
    ///
    /// On construction it parses `ast_text` once (using the command's
    /// stored parser limits). The parsed AST is exposed for reading or
    /// modifying. On destruction the (possibly modified) AST is formatted
    /// back into the owning `MutationCommand::ast_text`, so any changes
    /// made through the handle persist.
    ///
    /// All AST sub-fields (predicate, partition, update assignments) are
    /// accessed through the same handle, so a single `accessAst` call is
    /// enough to read several parts of the command without re-parsing.
    ///
    /// The handle is move-only. If constructed from a const `MutationCommand`
    /// it does not write anything back on destruction.
    class AccessedAst
    {
    public:
        AccessedAst(MutationCommand * owner, ASTPtr ast);
        ~AccessedAst() noexcept;

        AccessedAst(const AccessedAst &) = delete;
        AccessedAst & operator=(const AccessedAst &) = delete;
        AccessedAst(AccessedAst && other) noexcept;
        AccessedAst & operator=(AccessedAst && other) noexcept;

        explicit operator bool() const { return ast != nullptr; }

        ASTPtr getAstPtr() const { return ast; }
        ASTAlterCommand * operator->() const { return getAlter(); }
        ASTAlterCommand & operator*() const { return *getAlter(); }

        /// Clones of the corresponding sub-trees of the parsed alter command.
        /// They are returned by value (cloned) so they remain valid after
        /// the handle is destroyed.
        ASTPtr getPredicate() const;
        ASTPtr getPartition() const;
        std::unordered_map<String, ASTPtr> getColumnToUpdateExpression() const;

    private:
        MutationCommand * owner;
        ASTPtr ast;
        ASTAlterCommand * getAlter() const;
    };

    /// Parses `ast_text` once and returns a RAII handle. Modifications made
    /// through the handle are serialized back into `ast_text` when the handle
    /// is destroyed. Use the same handle to read multiple sub-trees and avoid
    /// re-parsing for each one.
    AccessedAst accessAst();
    AccessedAst accessAst() const;

    /// For MATERIALIZE INDEX and PROJECTION and STATISTICS
    String index_name = {};
    String projection_name = {};
    std::vector<String> statistics_columns = {};
    std::vector<String> statistics_types = {};

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

    /// If parse_alter_commands, than consider more Alter commands as mutation commands.
    /// `max_parser_depth` / `max_parser_backtracks` are captured into the returned
    /// command so subsequent on-demand re-parsing uses the same limits.
    static std::optional<MutationCommand> parse(
        const ASTAlterCommand & command,
        bool parse_alter_commands = false,
        bool with_pure_metadata_commands = false,
        UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH,
        UInt64 max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

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
    boost::intrusive_ptr<ASTExpressionList> ast(bool with_pure_metadata_commands = false) const;

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
