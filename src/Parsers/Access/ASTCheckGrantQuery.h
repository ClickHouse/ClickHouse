#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AccessRightsElement.h>


namespace DB
{

/** Parses queries like
  * CHECK GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}
  */
class ASTCheckGrantQuery : public IAST
{
public:
    AccessRightsElements access_rights_elements;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void replaceEmptyDatabase(const String & current_database);
    QueryKind getQueryKind() const override { return QueryKind::Check; }
};

}
