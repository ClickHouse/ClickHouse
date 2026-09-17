#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTShowTypesQuery final : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr like_pattern;

    String getID(char) const override { return "ShowTypesQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

    String getDatabase() const;
    void normalizeChildrenOrder();

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
