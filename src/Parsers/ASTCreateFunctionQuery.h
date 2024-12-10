#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTCreateFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr function_name;
    ASTPtr function_core;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateFunctionQuery" + (delim + getFunctionName()); }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateFunctionQuery>(clone()); }

    String getFunctionName() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }
};

}
