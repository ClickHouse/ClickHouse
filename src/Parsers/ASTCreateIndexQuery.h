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
    ASTPtr index_name;

    /// Stores the ASTIndexDeclaration here.
    ASTPtr index_decl;

    bool if_not_exists{false};
    bool unique{false};

    String getID(char delim) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTCreateIndexQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    /// Convert ASTCreateIndexQuery to ASTAlterCommand
    ASTPtr convertToASTAlterCommand() const;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
