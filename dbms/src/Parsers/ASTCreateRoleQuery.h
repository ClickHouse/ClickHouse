#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
  *
  * ALTER ROLE [IF EXISTS] name
  *      [RENAME TO new_name]
  */
class ASTCreateRoleQuery : public IAST
{
public:
    bool alter = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
