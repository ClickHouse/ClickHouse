#pragma once

#include <Parsers/IAST.h>
#include <Access/RowPolicy.h>
#include <utility>
#include <vector>


namespace DB
{
class ASTRoleList;

/** CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] name ON [database.]table
  *      [AS {PERMISSIVE | RESTRICTIVE}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING condition]
  *      [WITH CHECK condition] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER [ROW] POLICY [IF EXISTS] name ON [database.]table
  *      [RENAME TO new_name]
  *      [AS {PERMISSIVE | RESTRICTIVE}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING {condition | NONE}]
  *      [WITH CHECK {condition | NONE}] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ASTCreateRowPolicyQuery : public IAST
{
public:
    bool alter = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    RowPolicy::FullNameParts name_parts;
    String new_policy_name;

    std::optional<bool> is_restrictive;
    using ConditionIndex = RowPolicy::ConditionIndex;
    std::vector<std::pair<ConditionIndex, ASTPtr>> conditions;

    std::shared_ptr<ASTRoleList> roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
