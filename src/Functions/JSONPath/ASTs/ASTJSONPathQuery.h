#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathQuery : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathQuery"; }

    ASTPtr clone() const override { return make_intrusive<ASTJSONPathQuery>(*this); }
};

}
