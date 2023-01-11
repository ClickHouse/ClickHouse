#pragma once
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTDescribeCacheQuery : public ASTQueryWithOutput
{
public:
    String cache_name;

    String getID(char) const override { return "DescribeCacheQuery"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDescribeCacheQuery>(*this);
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DESCRIBE CACHE" << (settings.hilite ? hilite_none : "") << " " << cache_name;
    }
};

}
