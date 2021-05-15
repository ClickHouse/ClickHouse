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
        return "ASTJSONPath";
    }

    ASTPtr clone() const override
    {
        return std::make_shared<ASTJSONPath>(*this);
    }

    ASTJSONPathQuery * jsonpath_query;
};

}
