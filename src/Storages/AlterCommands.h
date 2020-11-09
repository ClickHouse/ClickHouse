#pragma once

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MutationCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterCommand;

/// Operation from the ALTER query (except for manipulation with PART/PARTITION).
/// Adding Nested columns is not expanded to add individual columns.
struct AlterCommand
{
    /// The AST of the whole command
    ASTPtr ast;

    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        MODIFY_ORDER_BY,
        ADD_INDEX,
        DROP_INDEX,
        ADD_CONSTRAINT,
        DROP_CONSTRAINT,
        MODIFY_TTL,
        MODIFY_SETTING,
        MODIFY_QUERY,
        RENAME_COLUMN,
    };

    Type type;

    String column_name;

    /// For DROP/CLEAR COLUMN/INDEX ... IN PARTITION
    ASTPtr partition;

    /// For ADD and MODIFY, a new column type.
    DataTypePtr data_type = nullptr;

    ColumnDefaultKind default_kind{};
    ASTPtr default_expression{};

    /// For COMMENT column
    std::optional<String> comment;

    /// For ADD or MODIFY - after which column to add a new one. If an empty string, add to the end.
    String after_column;

    /// For ADD_COLUMN, MODIFY_COLUMN - Add to the begin if it is true.
    bool first = false;

    /// For DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN
    bool if_exists = false;

    /// For ADD_COLUMN
    bool if_not_exists = false;

    /// For MODIFY_ORDER_BY
    ASTPtr order_by = nullptr;

    /// For ADD INDEX
    ASTPtr index_decl = nullptr;
    String after_index_name;

    /// For ADD/DROP INDEX
    String index_name;

    // For ADD CONSTRAINT
    ASTPtr constraint_decl = nullptr;

    // For ADD/DROP CONSTRAINT
    String constraint_name;

    /// For MODIFY TTL
    ASTPtr ttl = nullptr;

    /// indicates that this command should not be applied, for example in case of if_exists=true and column doesn't exist.
    bool ignore = false;

    /// Clear columns or index (don't drop from metadata)
    bool clear = false;

    /// For ADD and MODIFY
    CompressionCodecPtr codec = nullptr;

    /// For MODIFY SETTING
    SettingsChanges settings_changes;

    /// For MODIFY_QUERY
    ASTPtr select = nullptr;

    /// Target column name
    String rename_to;

    static std::optional<AlterCommand> parse(const ASTAlterCommand * command, bool sanity_check_compression_codecs);

    void apply(StorageInMemoryMetadata & metadata, const Context & context) const;

    /// Checks that alter query changes data. For MergeTree:
    ///    * column files (data and marks)
    ///    * each part meta (columns.txt)
    /// in each part on disk (it's not lightweight alter).
    bool isModifyingData(const StorageInMemoryMetadata & metadata) const;

    /// Check that alter command require data modification (mutation) to be
    /// executed. For example, cast from Date to UInt16 type can be executed
    /// without any data modifications. But column drop or modify from UInt16 to
    /// UInt32 require data modification.
    bool isRequireMutationStage(const StorageInMemoryMetadata & metadata) const;

    /// Checks that only settings changed by alter
    bool isSettingsAlter() const;

    /// Checks that only comment changed by alter
    bool isCommentAlter() const;

    /// Checks that any TTL changed by alter
    bool isTTLAlter(const StorageInMemoryMetadata & metadata) const;

    /// If possible, convert alter command to mutation command. In other case
    /// return empty optional. Some storages may execute mutations after
    /// metadata changes.
    std::optional<MutationCommand> tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, const Context & context) const;
};

/// Return string representation of AlterCommand::Type
String alterTypeToString(const AlterCommand::Type type);

class Context;

/// Vector of AlterCommand with several additional functions
class AlterCommands : public std::vector<AlterCommand>
{
private:
    bool prepared = false;

public:
    /// Validate that commands can be applied to metadata.
    /// Checks that all columns exist and dependecies between them.
    /// This check is lightweight and base only on metadata.
    /// More accurate check have to be performed with storage->checkAlterIsPossible.
    void validate(const StorageInMemoryMetadata & metadata, const Context & context) const;

    /// Prepare alter commands. Set ignore flag to some of them and set some
    /// parts to commands from storage's metadata (for example, absent default)
    void prepare(const StorageInMemoryMetadata & metadata);

    /// Apply all alter command in sequential order to storage metadata.
    /// Commands have to be prepared before apply.
    void apply(StorageInMemoryMetadata & metadata, const Context & context) const;

    /// At least one command modify data on disk.
    bool isModifyingData(const StorageInMemoryMetadata & metadata) const;

    /// At least one command modify settings.
    bool isSettingsAlter() const;

    /// At least one command modify comments.
    bool isCommentAlter() const;

    /// Return mutation commands which some storages may execute as part of
    /// alter. If alter can be performed as pure metadata update, than result is
    /// empty. If some TTL changes happened than, depending on materialize_ttl
    /// additional mutation command (MATERIALIZE_TTL) will be returned.
    MutationCommands getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, const Context & context) const;
};

}
