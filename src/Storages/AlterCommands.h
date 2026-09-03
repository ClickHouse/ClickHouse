#pragma once

#include <optional>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MutationCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterCommand;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Describes whether an ALTER requires rewriting existing parts.
/// Non-empty `lazy_settings` means that the on-disk representation changes without an immediate
/// mutation: old parts are converted on read and rewritten by later merges. Such conversions
/// require additional safety checks for metadata persisted in existing parts.
struct MutationStageDecision
{
    bool requires_mutation = false;
    std::set<std::string_view> lazy_settings;
};

/// Operation from the ALTER query (except for manipulation with PART/PARTITION).
/// Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    /// The AST of the whole command
    ASTPtr ast;

    enum Type
    {
        UNKNOWN,
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        MODIFY_ORDER_BY,
        MODIFY_SAMPLE_BY,
        ADD_INDEX,
        DROP_INDEX,
        ADD_CONSTRAINT,
        DROP_CONSTRAINT,
        MODIFY_CONSTRAINT,
        ADD_PROJECTION,
        DROP_PROJECTION,
        MODIFY_PROJECTION,
        ADD_STATISTICS,
        DROP_STATISTICS,
        MODIFY_STATISTICS,
        MODIFY_TTL,
        MODIFY_SETTING,
        RESET_SETTING,
        MODIFY_QUERY,
        MODIFY_REFRESH,
        RENAME_COLUMN,
        REMOVE_TTL,
        MODIFY_DATABASE_SETTING,
        MODIFY_DATABASE_COMMENT,
        COMMENT_TABLE,
        REMOVE_SAMPLE_BY,
        MODIFY_SQL_SECURITY,
    };

    /// Which property user wants to remove from column
    enum class RemoveProperty : uint8_t
    {
        NO_PROPERTY,
        /// Default specifiers
        DEFAULT,
        MATERIALIZED,
        ALIAS,

        /// Other properties
        COMMENT,
        CODEC,
        TTL,
        SETTINGS
    };

    Type type = UNKNOWN;

    String column_name;

    /// For DROP/CLEAR COLUMN/INDEX ... IN PARTITION
    ASTPtr partition;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type = nullptr;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};

    /// For COMMENT column or table
    std::optional<String> comment;

    /// For ADD or MODIFY - after which column to add a new one. If an empty string, add to the end.
    String after_column;

    /// For ADD_COLUMN, MODIFY_COLUMN, ADD_INDEX - Add to the begin if it is true.
    bool first = false;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN, RESET_SETTING
    bool if_exists = false;

    /// For ADD_COLUMN
    bool if_not_exists = false;

    /// For MODIFY_ORDER_BY
    ASTPtr order_by = nullptr;

    /// For MODIFY_SAMPLE_BY
    ASTPtr sample_by = nullptr;

    /// For ADD INDEX
    ASTPtr index_decl = nullptr;
    String after_index_name;

    /// For ADD/DROP INDEX
    String index_name;

    // For ADD/MODIFY CONSTRAINT
    ASTPtr constraint_decl = nullptr;

    // For ADD/DROP/MODIFY CONSTRAINT
    String constraint_name;

    /// For ADD PROJECTION
    ASTPtr projection_decl = nullptr;
    String after_projection_name;

    /// For ADD/DROP PROJECTION
    String projection_name;

    ASTPtr statistics_decl = nullptr;
    std::vector<String> statistics_columns;
    std::vector<String> statistics_types;

    /// For ADD COLUMN and MODIFY COLUMN: the column-level `STATISTICS(...)` clause of the column declaration
    ASTPtr column_statistics_decl = nullptr;

    /// For MODIFY TTL
    ASTPtr ttl = nullptr;

    /// indicates that this command should not be applied, for example in case of if_exists=true and column doesn't exist.
    bool ignore = false;

    /// Clear columns or index (don't drop from metadata)
    bool clear = false;

    /// For ADD and MODIFY
    ASTPtr codec = nullptr;

    /// For MODIFY SETTING or MODIFY COLUMN MODIFY SETTING
    SettingsChanges settings_changes;

    /// For RESET SETTING or MODIFY COLUMN RESET SETTING
    std::set<String> settings_resets;

    /// For MODIFY_QUERY
    ASTPtr select = nullptr;

    /// For MODIFY_SQL_SECURITY
    ASTPtr sql_security = nullptr;

    /// For MODIFY_REFRESH
    ASTPtr refresh = nullptr;

    ASTPtr add_enum_values = nullptr;

    /// Target column name
    String rename_to;

    /// What to remove from column (or TTL)
    RemoveProperty to_remove = RemoveProperty::NO_PROPERTY;

    /// Is this MODIFY COLUMN MODIFY SETTING or MODIFY COLUMN column with settings declaration)
    bool append_column_setting = false;

    static std::optional<AlterCommand> parse(const ASTAlterCommand * command);

    /// share_nested_offsets mirrors prepare()/validate(): when true, `n` and `n.*` are treated as
    /// the same logical column for IF NOT EXISTS existence checks; when false they are independent.
    /// `columns_before_alter` are the columns of the table before the whole ALTER (of which this command
    /// is a part) is applied; they let `MODIFY ORDER BY` suggest only the columns added by the ALTER for
    /// a typo, because an expression added to the sorting key may use nothing else.
    void apply(
        StorageInMemoryMetadata & metadata,
        ContextPtr context,
        bool share_nested_offsets = true,
        const ColumnsDescription * columns_before_alter = nullptr) const;

    /// Determines whether this command requires a mutation and identifies every setting
    /// that enables a matching lazy metadata conversion.
    MutationStageDecision getMutationStageDecision(const StorageInMemoryMetadata & metadata, const ContextPtr & context) const;

    /// Checks that only settings changed by alter
    bool isSettingsAlter() const;

    /// Checks that only comment changed by alter
    bool isCommentAlter() const;

    /// Checks that any TTL changed by alter
    bool isTTLAlter(const StorageInMemoryMetadata & metadata) const;

    /// Command removing some property from column or table
    bool isRemovingProperty() const;

    /// Checks that command will drop something or rename column.
    bool isDropOrRename() const;

    /// If possible, convert alter command to mutation command. In other case
    /// return empty optional. Some storages may execute mutations after
    /// metadata changes.
    /// share_nested_offsets is forwarded to the internal apply() so mutation-planning replay
    /// treats IF NOT EXISTS nested existence the same way as the real commands.apply().
    std::optional<MutationCommand> tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, ContextPtr context, bool share_nested_offsets = true) const;
};

class Context;

/// Vector of AlterCommand with several additional functions
class AlterCommands : public std::vector<AlterCommand>
{
private:
    bool prepared = false;

public:
    /// Validate that commands can be applied to metadata.
    /// Checks that all columns exist and dependencies between them.
    /// This check is lightweight and base only on metadata.
    /// More accurate check have to be performed with storage->checkAlterIsPossible.
    void validate(const StoragePtr & table, ContextPtr context) const;

    /// Prepare alter commands. Set ignore flag to some of them and set some
    /// parts to commands from storage's metadata (for example, absent default)
    void prepare(const StorageInMemoryMetadata & metadata, bool share_nested_offsets = true);

    /// Apply all alter command in sequential order to storage metadata.
    /// Commands have to be prepared before apply.
    /// share_nested_offsets is threaded to AlterCommand::apply so IF NOT EXISTS existence checks
    /// stay consistent with prepare()/validate() for nested columns (see AlterCommand::apply).
    void apply(StorageInMemoryMetadata & metadata, ContextPtr context, bool share_nested_offsets = true) const;

    /// At least one command modify settings or comments.
    bool hasNonReplicatedAlterCommand() const;

    /// All commands modify settings or comments.
    bool areNonReplicatedAlterCommands() const;

    /// All commands modify settings only.
    bool isSettingsAlter() const;

    /// All commands modify comments only.
    bool isCommentAlter() const;

    /// Return mutation commands which some storages may execute as part of
    /// alter. If alter can be performed as pure metadata update, than result is
    /// empty. If some TTL changes happened than, depending on materialize_ttl
    /// additional mutation command (MATERIALIZE_TTL) will be returned.
    /// share_nested_offsets is threaded to tryConvertToMutationCommand -> AlterCommand::apply so the
    /// intermediate metadata built while planning mutations matches the real commands.apply() for
    /// IF NOT EXISTS nested adds (see AlterCommand::apply).
    MutationCommands getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, ContextPtr context, bool with_alters=false, bool share_nested_offsets = true) const;

    /// Check if commands have a text index
    static bool hasTextIndex(const StorageInMemoryMetadata & metadata);

    /// Check if commands have any vector similarity index
    static bool hasVectorSimilarityIndex(const StorageInMemoryMetadata & metadata);
};

}
