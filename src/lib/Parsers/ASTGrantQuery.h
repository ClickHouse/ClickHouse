#pragma once

#include <Parsers/IAST.h>
#include <Access/AccessRightsElement.h>


namespace DB
{
class ASTRoleList;


/** GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO user_name
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO user_name
  */
class ASTGrantQuery : public IAST
{
public:
    enum class Kind
    {
        GRANT,
        REVOKE,
    };
    Kind kind = Kind::GRANT;
    AccessRightsElements access_rights_elements;
    std::shared_ptr<ASTRoleList> to_roles;
    bool grant_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
