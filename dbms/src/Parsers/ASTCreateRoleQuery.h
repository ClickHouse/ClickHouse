#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/// CREATE ROLE [IF NOT EXISTS] name [,...]
class ASTCreateRoleQuery : public IAST
{
public:
    bool if_not_exists = false;
    Strings role_names;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
