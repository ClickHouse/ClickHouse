#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    bool if_exists = false;

    String getID(char) const override { return "DropNamedCollectionQuery"; }

    ASTPtr clone() const override;

    /// `getID` returns a constant string that does not distinguish the dropped object, and
    /// `collection_name` / `if_exists` / the `ON CLUSTER` name are plain members, not part of
    /// `children` (this AST has none). Without folding them into the hash, `DROP NAMED COLLECTION a`
    /// and `DROP NAMED COLLECTION b` would share one tree hash. The rewrite-rule matcher treats an
    /// equal tree hash as semantic equality, so a rule template for one `DROP NAMED COLLECTION`
    /// would over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropNamedCollectionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
