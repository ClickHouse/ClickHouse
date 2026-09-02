#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Common/AccessEntityType.h>


namespace DB
{
class ASTRowPolicyNames;

/** MOVE {USER | ROLE | QUOTA | [ROW] POLICY | [SETTINGS] PROFILE} [IF EXISTS] name [,...] [ON [database.]table [,...]] TO storage_name
  */
class ASTMoveAccessEntityQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    AccessEntityType type{};
    Strings names;
    boost::intrusive_ptr<ASTRowPolicyNames> row_policy_names;

    String storage_name;

    String getID(char) const override;
    ASTPtr clone() const override;

    /// `getID` distinguishes only the entity type; the moved names, the target storage and the
    /// `ON CLUSTER` name are plain members, not part of `children` (this AST has none). Without
    /// folding them into the hash, `MOVE USER a TO s1` and `MOVE USER b TO s2` would share one
    /// tree hash. The rewrite-rule matcher treats an equal tree hash as semantic equality, so a
    /// rule template for one `MOVE` would over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTMoveAccessEntityQuery>(clone()); }

    void replaceEmptyDatabase(const String & current_database) const;

    QueryKind getQueryKind() const override { return QueryKind::Move; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
