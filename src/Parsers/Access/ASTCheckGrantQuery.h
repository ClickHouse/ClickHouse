#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include "Parsers/ASTQueryParameter.h"


namespace DB
{
class ASTRolesOrUsersSet;


/** Parses queries like
  * CHECK GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}
  */
class ASTCheckGrantQuery : public IAST
{
public:
    AccessRightsElements access_rights_elements;

    String getID(char) const override;
    ASTPtr clone() const override;
    void replaceEmptyDatabase(const String & current_database);
    QueryKind getQueryKind() const override { return QueryKind::Check; }
};
}
