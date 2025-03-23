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

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
