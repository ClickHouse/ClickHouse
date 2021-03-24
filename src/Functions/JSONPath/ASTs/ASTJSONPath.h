#pragma once

#include <Parsers/IAST.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>

namespace DB
{
class ASTJSONPath : public IAST
{
public:
    String getID(char) const override
    {
        std::cerr << "in ASTJSONPath: getID\n";
        return "ASTJSONPath";
    }

    ASTPtr clone() const override
    {
        std::cerr << "in " << "ASTJSONPath" << ": clone\n";
        return std::make_shared<ASTJSONPath>(*this);
    }

    ASTJSONPathQuery * jsonpath_query;
};

}
