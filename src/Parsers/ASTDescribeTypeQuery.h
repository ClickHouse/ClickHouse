#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTDescribeTypeQuery final : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr type_name;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Describe; }

    String getDatabase() const;
    String getTypeName() const;
    void normalizeChildrenOrder();

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
