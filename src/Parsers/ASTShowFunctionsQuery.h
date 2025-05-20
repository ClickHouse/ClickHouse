#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTShowFunctionsQuery : public ASTQueryWithOutput
{
public:
    bool case_insensitive_like = false;
    String like;

    String getID(char) const override { return "ShowFunctions"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
