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

class ASTAlterQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    enum ParameterType
    {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        MODIFY_PRIMARY_KEY,

        DROP_PARTITION,
        ATTACH_PARTITION,
        FETCH_PARTITION,
        FREEZE_PARTITION,

        NO_TYPE,
    };

    struct Parameters
    {
        Parameters();

        int type = NO_TYPE;

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

        /** In DROP PARTITION and RESHARD PARTITION queries, the value or ID of the partition is stored here.
          */
        ASTPtr partition;

        bool detach = false;        /// true for DETACH PARTITION

        bool part = false;          /// true for ATTACH PART

        bool do_copy = false;       /// for RESHARD PARTITION

        bool clear_column = false;  /// for CLEAR COLUMN (do not drop column from metadata)

        /** For FETCH PARTITION - the path in ZK to the shard, from which to download the partition.
          */
        String from;

        /** For FREEZE PARTITION - place local backup to directory with specified name.
          */
        String with_name;

        /// deep copy
        void clone(Parameters & p) const;
    };

    using ParameterContainer = std::vector<Parameters>;
    ParameterContainer parameters;
    String database;
    String table;


    void addParameters(const Parameters & params);

    /** Get the text that identifies this element. */
    String getID() const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
