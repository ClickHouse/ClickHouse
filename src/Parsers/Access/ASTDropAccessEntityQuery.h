#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Common/AccessEntityType.h>


namespace DB
{
class ASTRowPolicyNames;
struct MaskingPolicyName;

/** DROP USER [IF EXISTS] name [,...]
  * DROP ROLE [IF EXISTS] name [,...]
  * DROP QUOTA [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  * DROP MASKING POLICY [IF EXISTS] name ON [database.]table
  * DROP [SETTINGS] PROFILE [IF EXISTS] name [,...]
  */
class ASTDropAccessEntityQuery final : public IAST, public ASTQueryWithOnCluster
{
public:
    AccessEntityType type{};
    bool if_exists = false;
    Strings names;
    String storage_name;
    boost::intrusive_ptr<ASTRowPolicyNames> row_policy_names;
    std::shared_ptr<MaskingPolicyName> masking_policy_name;

    String getID(char) const override;
    ASTPtr clone() const override;

    /// `getID` distinguishes only the entity type; the dropped names, `if_exists`, the source
    /// storage and the `ON CLUSTER` name are plain members, not part of `children` (this AST has
    /// none). Without folding them into the hash, `DROP USER a` and `DROP USER b` would share one
    /// tree hash. The rewrite-rule matcher treats an equal tree hash as semantic equality, so a
    /// rule template for one `DROP` would over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropAccessEntityQuery>(clone()); }

    void replaceEmptyDatabase(const String & current_database) const;

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
