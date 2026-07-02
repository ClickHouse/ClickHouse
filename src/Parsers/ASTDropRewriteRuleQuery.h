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

    /// `rule_name` is the only field that distinguishes one `DROP RULE` from another,
    /// but it is not part of `children`, so the default hash (just `getID`) is identical
    /// for every `DROP RULE`. The rewrite-rule matcher compares tree hashes, so without
    /// this override a rule whose source template is `DROP RULE a` would match
    /// `DROP RULE b`. Fold `rule_name` into the hash to keep matching exact.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

using ASTDropRewriteRuleQueryPtr = std::shared_ptr<ASTDropRewriteRuleQuery>;

}
