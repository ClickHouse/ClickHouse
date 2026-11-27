#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{
/// DELETE FROM [db.]name WHERE ...
class ASTDeleteQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;
    QueryKind getQueryKind() const override { return QueryKind::Delete; }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTDeleteQuery>(clone(), params.default_database);
    }

    /** Used in DELETE FROM queries.
     *  The value or ID of the partition is stored here.
     */
    ASTPtr partition;

    ASTPtr predicate;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
