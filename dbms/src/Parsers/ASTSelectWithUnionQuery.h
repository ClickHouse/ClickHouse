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
    String getID(char) const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr list_of_selects;
};

}
