#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathStar : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathStar"; }

    ASTPtr clone() const override { return make_intrusive<ASTJSONPathStar>(*this); }
};

}
