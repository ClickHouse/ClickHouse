#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTAlterRewriteRuleQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String rule_name;
    String reject_message;
    String whole_query;
    ASTPtr source_query;
    ASTPtr resulting_query;

    String getID(char) const override { return "AlterRewriteRuleQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTAlterRewriteRuleQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

    bool hasSecretParts() const override { return false; }

    bool rewrite() const { if (resulting_query) { return true; } return false; }
    bool reject()  const { return !reject_message.empty(); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

using ASTAlterRewriteRuleQueryPtr = std::shared_ptr<ASTAlterRewriteRuleQuery>;

}
