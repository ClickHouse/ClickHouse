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
 *  ALTER LIVE VIEW [db.]name_type
 *      REFRESH
 */

class ASTAlterCommand : public IAST
{
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

        DROP_PARTITION,
        DROP_DETACHED_PARTITION,
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

        NO_TYPE,

        LIVE_VIEW_REFRESH,

        MODIFY_DATABASE_SETTING,

        MODIFY_COMMENT,
    };

    Type type = NO_TYPE;

    /** The ADD COLUMN query stores the name and type of the column to add
     *  This field is not used in the DROP query
     *  In MODIFY query, the column name and the new type are stored here
     */
    ASTPtr col_decl;

    /** The ADD COLUMN and MODIFY COLUMN query here optionally stores the name of the column following AFTER
     * The DROP query stores the column name for deletion here
     * Also used for RENAME COLUMN.
     */
    ASTPtr column;

    /** For MODIFY ORDER BY
     */
    ASTPtr order_by;

    /** For MODIFY SAMPLE BY
     */
    ASTPtr sample_by;

    /** The ADD INDEX query stores the IndexDeclaration there.
     */
    ASTPtr index_decl;

    /** The ADD INDEX query stores the name of the index following AFTER.
     *  The DROP INDEX query stores the name for deletion.
     *  The MATERIALIZE INDEX query stores the name of the index to materialize.
     *  The CLEAR INDEX query stores the name of the index to clear.
     */
    ASTPtr index;

    /** The ADD CONSTRAINT query stores the ConstraintDeclaration there.
    */
    ASTPtr constraint_decl;

    /** The DROP CONSTRAINT query stores the name for deletion.
    */
    ASTPtr constraint;

    /** The ADD PROJECTION query stores the ProjectionDeclaration there.
     */
    ASTPtr projection_decl;

    /** The ADD PROJECTION query stores the name of the projection following AFTER.
     *  The DROP PROJECTION query stores the name for deletion.
     *  The MATERIALIZE PROJECTION query stores the name of the projection to materialize.
     *  The CLEAR PROJECTION query stores the name of the projection to clear.
     */
    ASTPtr projection;

    /** Used in DROP PARTITION, ATTACH PARTITION FROM, UPDATE, DELETE queries.
     *  The value or ID of the partition is stored here.
     */
    ASTPtr partition;

    /// For DELETE/UPDATE WHERE: the predicate that filters the rows to delete/update.
    ASTPtr predicate;

    /// A list of expressions of the form `column = expr` for the UPDATE command.
    ASTPtr update_assignments;

    /// A column comment
    ASTPtr comment;

    /// For MODIFY TTL query
    ASTPtr ttl;

    /// FOR MODIFY_SETTING
    ASTPtr settings_changes;

    /// FOR RESET_SETTING
    ASTPtr settings_resets;

    /// For MODIFY_QUERY
    ASTPtr select;

    /** In ALTER CHANNEL, ADD, DROP, SUSPEND, RESUME, REFRESH, MODIFY queries, the list of live views is stored here
     */
    ASTPtr values;

    bool detach = false;        /// true for DETACH PARTITION

    bool part = false;          /// true for ATTACH PART, DROP DETACHED PART and MOVE

    bool clear_column = false;  /// for CLEAR COLUMN (do not drop column from metadata)

    bool clear_index = false;   /// for CLEAR INDEX (do not drop index from metadata)

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

    /// Target column name
    ASTPtr rename_to;

    /// Which property user want to remove
    String remove_property;

    String getID(char delim) const override;

    ASTPtr clone() const override;

    static const char * typeToString(Type type);

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    enum class AlterObjectType
    {
        TABLE,
        DATABASE,
        LIVE_VIEW,
        UNKNOWN,
    };

    AlterObjectType alter_object = AlterObjectType::UNKNOWN;

    ASTExpressionList * command_list = nullptr;

    bool isSettingsAlter() const;

    bool isFreezeAlter() const;

    bool isAttachAlter() const;

    bool isFetchAlter() const;

    bool isDropPartitionAlter() const;

    String getID(char) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTAlterQuery>(clone(), new_database);
    }

    virtual QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    bool isOneCommandTypeOnly(const ASTAlterCommand::Type & type) const;
};

}
