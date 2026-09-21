#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropWorkloadQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String workload_name;

    bool if_exists = false;

    String getID(char) const override { return "DropWorkloadQuery"; }

    ASTPtr clone() const override;

    /// `getID` returns a constant string that does not distinguish the dropped object, and
    /// `workload_name` / `if_exists` / the `ON CLUSTER` name are plain members, not part of
    /// `children` (this AST has none). Without folding them into the hash, `DROP WORKLOAD a` and
    /// `DROP WORKLOAD b` would share one tree hash. The rewrite-rule matcher treats an equal tree
    /// hash as semantic equality, so a rule template for one `DROP WORKLOAD` would over-match an
    /// unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropWorkloadQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
