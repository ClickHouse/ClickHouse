#pragma once

#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathRoot : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathRoot"; }

    ASTPtr clone() const override { return make_intrusive<ASTJSONPathRoot>(*this); }
};

}
