#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>

namespace DB
{


/** DEDUCE query
  */
class ASTDeduceQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    ASTPtr col_to_deduce;


    String getID(char delim) const override { return "DeduceQuery" + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDeduceQuery>(*this);
        res->children.clear();

        return res;
    }

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTDeduceQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Deduce; }
};

}
