#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTDescribeCacheQuery : public ASTQueryWithOutput
{
public:
    String cache_name;

    String getID(char) const override;
    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Describe; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
