#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropRewriteRuleQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String rule_name;

    String getID(char) const override { return "DropRewriteRuleQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropRewriteRuleQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

    bool hasSecretParts() const override { return false; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

using ASTDropRewriteRuleQueryPtr = std::shared_ptr<ASTDropRewriteRuleQuery>;

}
