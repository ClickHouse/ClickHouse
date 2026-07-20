#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPath : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPath"; }

    ASTPtr clone() const override { return std::make_shared<ASTJSONPath>(*this); }

    ASTJSONPathQuery * jsonpath_query;
};

}
