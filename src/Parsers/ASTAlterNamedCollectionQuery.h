#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    SettingsChanges changes;
    std::vector<std::string> delete_keys;
    bool if_exists = false;
    std::unordered_map<String, bool> overridability;

    String getID(char) const override { return "AlterNamedCollectionQuery"; }

    ASTPtr clone() const override;

    /// `getID` returns a constant string that does not distinguish the altered collection, and
    /// `collection_name` / `changes` / `delete_keys` / `if_exists` / the `ON CLUSTER` name are
    /// plain members, not part of `children` (this AST has none). Without folding them into the
    /// hash, `ALTER NAMED COLLECTION a SET x = 1` and an unrelated `ALTER NAMED COLLECTION` would
    /// share one tree hash. The rewrite-rule matcher treats an equal tree hash as semantic
    /// equality, so a rule template for one `ALTER NAMED COLLECTION` would over-match another.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTAlterNamedCollectionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

    bool hasSecretParts() const override { return true; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
