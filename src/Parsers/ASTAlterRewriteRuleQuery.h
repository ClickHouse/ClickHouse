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
    /// True iff the rule was created with `REJECT WITH ...`. Tracked
    /// independently of `reject_message` so that `REJECT WITH ''` is still
    /// a rejecting rule (with an empty user-facing message) rather than a
    /// silent no-op.
    bool is_reject = false;

    String getID(char) const override { return "AlterRewriteRuleQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTAlterRewriteRuleQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

    /// `source_query` and `resulting_query` can contain arbitrary nested queries
    /// (e.g. table functions or settings holding secrets) and are not part of
    /// `children`, so inspect them explicitly to keep AST secret masking working.
    bool hasSecretParts() const override
    {
        return (source_query && source_query->hasSecretParts())
            || (resulting_query && resulting_query->hasSecretParts());
    }

    bool rewrite() const { if (resulting_query) { return true; } return false; }
    bool reject()  const { return is_reject; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

using ASTAlterRewriteRuleQueryPtr = std::shared_ptr<ASTAlterRewriteRuleQuery>;

}
