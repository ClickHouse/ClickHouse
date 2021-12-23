#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String function_name;

    bool if_exists = false;

    String getID(char) const override { return "DropFunctionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropFunctionQuery>(clone()); }
};

}
