#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathQuery : public IAST
{
public:
    String getID(char) const override
    {
        std::cerr << "in ASTJSONPathQuery: getID\n";
        return "ASTJSONPathQuery";
    }

    ASTPtr clone() const override
    {
        std::cerr << "in " << getID(' ') << ": clone\n";
        return std::make_shared<ASTJSONPathQuery>(*this);
    }
};

}
