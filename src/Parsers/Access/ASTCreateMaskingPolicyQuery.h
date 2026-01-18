#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/IAST.h>

namespace DB
{

/** CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] name ON [database.]table
  *      UPDATE expression
  *      [WHERE condition]
  *      TO {role/user [,...] | ALL | ALL EXCEPT role/user [,...]}
  *      [PRIORITY x]
  *
  * ALTER MASKING POLICY [IF EXISTS] name ON [database.]table
  *      [RENAME TO new_name]
  *      [UPDATE expression]
  *      [WHERE condition]
  *      [TO {role/user [,...] | ALL | ALL EXCEPT role/user [,...]}]
  *      [PRIORITY x]
  */
class ASTCreateMaskingPolicyQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    String storage_name;

    String name;
    String database;
    String table_name;

    String new_name;

    std::shared_ptr<ASTRolesOrUsersSet> roles;

    ASTPtr update_assignments;  // ASTExpressionList of ASTAssignment objects
    ASTPtr where_condition;

    Int64 priority;

    String getID(char) const override;
    ASTPtr clone() const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateMaskingPolicyQuery>(clone()); }

    void replaceCurrentUserTag(const String & current_user_name) const;
    void replaceEmptyDatabase(const String & current_database) const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
