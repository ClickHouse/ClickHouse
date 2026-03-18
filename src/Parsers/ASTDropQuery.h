#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** DROP query
  */
class ASTDropQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    enum Kind
    {
        Drop,
        Detach,
        Truncate,
    };

    Kind kind;
    bool if_exists{false};
    bool if_empty{false};

    /// Useful if we already have a DDL lock
    bool no_ddl_lock{false};

    /// For `TRUNCATE ALL TABLES` query
    bool has_all_tables{false};

    /// We dropping dictionary, so print correct word
    bool is_dictionary{false};

    /// Same as above
    bool is_view{false};

    bool sync{false};

    /// We detach the object permanently, so it will not be reattached back during server restart.
    bool permanently{false};

    /// Used to drop multiple tables only, example: DROP TABLE t1, t2, t3...
    ASTPtr database_and_tables;

    /// Get the text that identifies this element.
    String getID(char) const override;
    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTDropQuery>(clone(), params.default_database);
    }

    /// Convert an AST that deletes multiple tables into multiple ASTs that delete a single table.
    ASTs getRewrittenASTsOfSingleTable();

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
