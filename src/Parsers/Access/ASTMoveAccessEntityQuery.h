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
    AccessEntityType type;
    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;

    String storage_name;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTMoveAccessEntityQuery>(clone()); }

    void replaceEmptyDatabase(const String & current_database) const;

    QueryKind getQueryKind() const override { return QueryKind::Move; }
};
}
