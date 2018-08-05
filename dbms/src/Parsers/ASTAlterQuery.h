#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** ALTER query:
 *  ALTER TABLE [db.]name_type
 *      ADD COLUMN col_name type [AFTER col_after],
 *      DROP COLUMN col_drop [FROM PARTITION partition],
 *      MODIFY COLUMN col_name type,
 *      DROP PARTITION partition,
 */

class ASTAlterCommand : public IAST
{
public:
    enum Type
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        MODIFY_PRIMARY_KEY,

        DROP_PARTITION,
        ATTACH_PARTITION,
        REPLACE_PARTITION,
        FETCH_PARTITION,
        FREEZE_PARTITION,

        DELETE,

        NO_TYPE,
    };

    Type type = NO_TYPE;

    /** The ADD COLUMN query stores the name and type of the column to add
     *  This field is not used in the DROP query
     *  In MODIFY query, the column name and the new type are stored here
     */
    ASTPtr col_decl;

    /** The ADD COLUMN query here optionally stores the name of the column following AFTER
     * The DROP query stores the column name for deletion here
     */
    ASTPtr column;

    /** For MODIFY PRIMARY KEY
     */
    ASTPtr primary_key;

    /** Used in DROP PARTITION and ATTACH PARTITION FROM queries.
     *  The value or ID of the partition is stored here.
     */
    ASTPtr partition;

    /// For DELETE WHERE: the predicate that filters the rows to delete.
    ASTPtr predicate;

    bool detach = false;        /// true for DETACH PARTITION

    bool part = false;          /// true for ATTACH PART

    bool clear_column = false;  /// for CLEAR COLUMN (do not drop column from metadata)

    /** For FETCH PARTITION - the path in ZK to the shard, from which to download the partition.
     */
    String from;

    /** For FREEZE PARTITION - place local backup to directory with specified name.
     */
    String with_name;

    /// REPLACE(ATTACH) PARTITION partition FROM db.table
    String from_database;
    String from_table;
    /// To distinguish REPLACE and ATTACH PARTITION partition FROM db.table
    bool replace = true;

    String getID() const override { return "AlterCommand_" + std::to_string(static_cast<int>(type)); }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterCommandList : public IAST
{
public:
    std::vector<ASTAlterCommand *> commands;

    void add(const ASTPtr & command)
    {
        commands.push_back(static_cast<ASTAlterCommand *>(command.get()));
        children.push_back(command);
    }

    String getID() const override { return "AlterCommandList"; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    String database;
    String table;

    ASTAlterCommandList * command_list = nullptr;

    String getID() const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
