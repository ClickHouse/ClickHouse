#pragma once

#include <optional>
#include <vector>
#include <memory>
#include <unordered_map>

#include <Core/Defines.h>
#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class Context;
class WriteBuffer;
class ReadBuffer;

/// Represents set of actions which should be applied
/// to values from set of columns which satisfy predicate.
struct MutationCommand
{
    /// Serialized text of the command (output of `formatWithSecretsOneLine`),
    /// used for on-disk / ZooKeeper persistence.
    String ast_text = {};

    /// Parser limits used when `ast_text` was produced. Re-applied if the
    /// AST has to be reparsed from `ast_text`.
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

    /// Returns the parsed alter command (shareable, const). The first call
    /// parses `ast_text` and caches the result; further calls return the cache.
    /// Returns nullptr if `ast_text` is empty.
    boost::intrusive_ptr<const ASTAlterCommand> ast() const;

    /// Set the cached AST and refresh `ast_text` from it. The pointer must be
    /// non-null. Use this when the AST is already in hand to skip parsing on
    /// the first `ast()` call.
    void setAst(boost::intrusive_ptr<ASTAlterCommand> alter);

    /// RAII handle for editing the AST of a `MutationCommand` in place. The
    /// constructor takes a mutable copy of the AST. To publish edits, call
    /// `commit` (it serializes the AST back into `ast_text` and refreshes the
    /// cache, and may throw); otherwise the destructor discards them.
    /// Neither copyable nor movable.
    class MutableAst
    {
    public:
        explicit MutableAst(MutationCommand & owner);
        ~MutableAst() noexcept;

        MutableAst(const MutableAst &) = delete;
        MutableAst & operator=(const MutableAst &) = delete;
        MutableAst(MutableAst &&) = delete;
        MutableAst & operator=(MutableAst &&) = delete;

        ASTAlterCommand & operator*() const { return *ast; }
        ASTAlterCommand * operator->() const { return ast.get(); }
        ASTAlterCommand * get() const { return ast.get(); }

        /// Serialize the (possibly modified) AST back to `owner.ast_text` and
        /// refresh `owner.cached_ast`. May throw.
        void commit();

    private:
        MutationCommand & owner;
        boost::intrusive_ptr<ASTAlterCommand> ast;
        bool committed = false;
    };

    /// Open a mutating scope on the AST. See `MutableAst` for the semantics.
    MutableAst mutateAst() { return MutableAst(*this); }

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

    /// If `parse_alter_commands` is true, more alter commands are accepted as
    /// mutation commands. `max_parser_depth` / `max_parser_backtracks` are
    /// captured into the returned command so subsequent on-demand re-parsing
    /// uses the same limits as this initial parse.
    static std::optional<MutationCommand> parse(
        const ASTAlterCommand & command,
        bool parse_alter_commands = false,
        bool with_pure_metadata_commands = false,
        UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH,
        UInt64 max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    /// Lazily populated by `ast`. Hot paths re-read this without re-parsing.
    /// Set eagerly by `setAst` / `MutableAst::commit` when an AST is in hand.
    /// Implementation detail — prefer `ast` / `setAst` over touching it directly.
    mutable boost::intrusive_ptr<const ASTAlterCommand> cached_ast = {};

    /// This command shouldn't stick with other commands
    bool isBarrierCommand() const;
    bool isPureMetadataCommand() const;
    bool isEmptyCommand() const;
    bool isDropOrRename() const;
    bool affectsAllColumns() const;
};

/// Collect the `UPDATE column = expr, ...` assignments from a parsed alter
/// command into a `column -> expression` map. The returned `ASTPtr`s point
/// into the assignment subtrees of `alter`, so the map is valid for as long
/// as `alter` is.
std::unordered_map<String, ASTPtr> getColumnToUpdateExpression(const ASTAlterCommand & alter);

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
