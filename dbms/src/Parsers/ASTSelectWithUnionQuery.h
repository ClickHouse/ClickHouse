#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/** Single SELECT query or multiple SELECT queries with UNION ALL.
  * Only UNION ALL is possible. No UNION DISTINCT or plain UNION.
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID() const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void setDatabaseIfNeeded(const String & database_name);

    ASTPtr list_of_selects;
};

}
