#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>


namespace DB
{

/** CREATE INDEX [IF NOT EXISTS] name ON [db].name (expression) TYPE type GRANULARITY value
 */

class ASTCreateIndexQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool if_not_exists{false};

    ASTPtr index_name;

    /// Stores the IndexDeclaration here.
    ASTPtr index_decl;

    String getID(char delim) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTCreateIndexQuery>(clone(), params.default_database);
    }

    virtual QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
