#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/IAST.h>


namespace DB
{

/** ALTER query:
 *  ALTER TABLE [db.]name_type
 *      ADD COLUMN col_name type [AFTER col_after],
 *      DROP COLUMN col_drop [FROM PARTITION partition],
 *      MODIFY COLUMN col_name type,
 *      DROP PARTITION partition,
 *      COMMENT_COLUMN col_name 'comment',
 */

class ASTAlterCommand : public IAST
{
    friend class ASTAlterQuery;

public:
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        COMMENT_COLUMN,
        RENAME_COLUMN,
        MATERIALIZE_COLUMN,

        MODIFY_ORDER_BY,
        MODIFY_SAMPLE_BY,
        MODIFY_TTL,
        MATERIALIZE_TTL,
        MODIFY_SETTING,
        RESET_SETTING,
        MODIFY_QUERY,
        MODIFY_REFRESH,
        REMOVE_TTL,
        REMOVE_SAMPLE_BY,

        ADD_INDEX,
        DROP_INDEX,
        MATERIALIZE_INDEX,

        ADD_CONSTRAINT,
        DROP_CONSTRAINT,

        ADD_PROJECTION,
        DROP_PROJECTION,
        MATERIALIZE_PROJECTION,

        ADD_STATISTICS,
        DROP_STATISTICS,
        MODIFY_STATISTICS,
        MATERIALIZE_STATISTICS,

        DROP_PARTITION,
        DROP_DETACHED_PARTITION,
        FORGET_PARTITION,
        ATTACH_PARTITION,
        MOVE_PARTITION,
        REPLACE_PARTITION,
        FETCH_PARTITION,
        FREEZE_PARTITION,
        FREEZE_ALL,
        UNFREEZE_PARTITION,
        UNFREEZE_ALL,

        DELETE,
        UPDATE,
        APPLY_DELETED_MASK,

        NO_TYPE,

        MODIFY_DATABASE_SETTING,

        MODIFY_COMMENT,
        MODIFY_SQL_SECURITY,
    };

    Type type = NO_TYPE;

    /** The ADD COLUMN query stores the name and type of the column to add
     *  This field is not used in the DROP query
     *  In MODIFY query, the column name and the new type are stored here
     */
    IAST * col_decl = nullptr;

    /** The ADD COLUMN and MODIFY COLUMN query here optionally stores the name of the column following AFTER
     * The DROP query stores the column name for deletion here
     * Also used for RENAME COLUMN.
     */
    IAST * column = nullptr;

    /** For MODIFY ORDER BY
     */
    IAST * order_by = nullptr;

    /** For MODIFY SAMPLE BY
     */
    IAST * sample_by = nullptr;

    /** The ADD INDEX query stores the IndexDeclaration there.
     */
    IAST * index_decl = nullptr;

    /** The ADD INDEX query stores the name of the index following AFTER.
     *  The DROP INDEX query stores the name for deletion.
     *  The MATERIALIZE INDEX query stores the name of the index to materialize.
     *  The CLEAR INDEX query stores the name of the index to clear.
     */
    IAST * index = nullptr;

    /** The ADD CONSTRAINT query stores the ConstraintDeclaration there.
    */
    IAST * constraint_decl = nullptr;

    /** The DROP CONSTRAINT query stores the name for deletion.
    */
    IAST * constraint = nullptr;

    /** The ADD PROJECTION query stores the ProjectionDeclaration there.
     */
    IAST * projection_decl = nullptr;

    /** The ADD PROJECTION query stores the name of the projection following AFTER.
     *  The DROP PROJECTION query stores the name for deletion.
     *  The MATERIALIZE PROJECTION query stores the name of the projection to materialize.
     *  The CLEAR PROJECTION query stores the name of the projection to clear.
     */
    IAST * projection = nullptr;

    IAST * statistics_decl = nullptr;

    /** Used in DROP PARTITION, ATTACH PARTITION FROM, FORGET PARTITION, UPDATE, DELETE queries.
     *  The value or ID of the partition is stored here.
     */
    IAST * partition = nullptr;

    /// For DELETE/UPDATE WHERE: the predicate that filters the rows to delete/update.
    IAST * predicate = nullptr;

    /// A list of expressions of the form `column = expr` for the UPDATE command.
    IAST * update_assignments = nullptr;

    /// A column comment
    IAST * comment = nullptr;

    /// For MODIFY TTL query
    IAST * ttl = nullptr;

    /// FOR MODIFY_SETTING
    IAST * settings_changes = nullptr;

    /// FOR RESET_SETTING
    IAST * settings_resets = nullptr;

    /// For MODIFY_QUERY
    IAST * select = nullptr;

    /// For MODIFY_SQL_SECURITY
    IAST * sql_security = nullptr;

    /// Target column name
    IAST * rename_to = nullptr;

    /// For MODIFY REFRESH
    ASTPtr refresh;

    bool detach = false;        /// true for DETACH PARTITION

    bool part = false;          /// true for ATTACH PART, DROP DETACHED PART and MOVE

    bool clear_column = false;  /// for CLEAR COLUMN (do not drop column from metadata)

    bool clear_index = false;   /// for CLEAR INDEX (do not drop index from metadata)

    bool clear_statistics = false;   /// for CLEAR STATISTICS (do not drop statistics from metadata)

    bool clear_projection = false;   /// for CLEAR PROJECTION (do not drop projection from metadata)

    bool if_not_exists = false; /// option for ADD_COLUMN

    bool if_exists = false;     /// option for DROP_COLUMN, MODIFY_COLUMN, COMMENT_COLUMN

    bool first = false;         /// option for ADD_COLUMN, MODIFY_COLUMN

    DataDestinationType move_destination_type; /// option for MOVE PART/PARTITION

    String move_destination_name;             /// option for MOVE PART/PARTITION

    /** For FETCH PARTITION - the path in ZK to the shard, from which to download the partition.
     */
    String from;

    /**
     * For FREEZE PARTITION - place local backup to directory with specified name.
     * For UNFREEZE - delete local backup at directory with specified name.
     */
    String with_name;

    /// REPLACE(ATTACH) PARTITION partition FROM db.table
    String from_database;
    String from_table;
    /// To distinguish REPLACE and ATTACH PARTITION partition FROM db.table
    bool replace = true;
    /// MOVE PARTITION partition TO TABLE db.table
    String to_database;
    String to_table;

    /// Which property user want to remove
    String remove_property;

    String getID(char delim) const override;

    ASTPtr clone() const override;

    // This function is only meant to be called during application startup
    // For reasons see https://github.com/ClickHouse/ClickHouse/pull/59532
    static void setFormatAlterCommandsWithParentheses(bool value) { format_alter_commands_with_parentheses = value; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override;

    static inline bool format_alter_commands_with_parentheses = false;
};

class ASTAlterQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    enum class AlterObjectType : uint8_t
    {
        TABLE,
        DATABASE,
        UNKNOWN,
    };

    AlterObjectType alter_object = AlterObjectType::UNKNOWN;

    ASTExpressionList * command_list = nullptr;

    bool isSettingsAlter() const;

    bool isFreezeAlter() const;

    bool isAttachAlter() const;

    bool isFetchAlter() const;

    bool isDropPartitionAlter() const;

    bool isMovePartitionToDiskOrVolumeAlter() const;

    bool isCommentAlter() const;

    String getID(char) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTAlterQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    bool isOneCommandTypeOnly(const ASTAlterCommand::Type & type) const;

    void forEachPointerToChild(std::function<void(void**)> f) override;
};

}
