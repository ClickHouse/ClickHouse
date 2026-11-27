#pragma once

#include <Functions/JSONPath/ASTs/ASTJSONPathQuery.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPath : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPath"; }

    ASTPtr clone() const override { return make_intrusive<ASTJSONPath>(*this); }

    ASTJSONPathQuery * jsonpath_query;
};

}
