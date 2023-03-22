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
    void formatQueryImpl(FormattingBuffer out) const override
    {
        out.writeKeyword("DESCRIBE FILESYSTEM CACHE ");
        out.ostr << cache_name;
    }
};

}
