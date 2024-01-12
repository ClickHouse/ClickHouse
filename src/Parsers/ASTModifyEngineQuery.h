#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTModifyEngineQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool to_replicated = true;

    String getID(char) const override;

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTModifyEngineQuery>(clone(), params.default_database);
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
