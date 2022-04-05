#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTCreateFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr function_name;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateFunctionQuery" + (delim + getFunctionName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateFunctionQuery>(clone()); }

    String getFunctionName() const;
};

class ASTCreateLambdaFunctionQuery : public ASTCreateFunctionQuery
{
public:
    ASTPtr function_core;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
    ASTPtr clone() const override;
};

class ASTCreateInterpFunctionQuery : public ASTCreateFunctionQuery
{
public:
    ASTPtr function_args;
    ASTPtr function_body;
    ASTPtr interpreter_name;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
    ASTPtr clone() const override;
};

}
