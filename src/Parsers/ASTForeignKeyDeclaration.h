#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/*
 * Currently ignore the foreign key node, flesh it out when needed
 */
class ASTForeignKeyDeclaration : public IAST
{
public:
    String name;

    String getID(char) const override { return "Foreign Key"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTForeignKeyDeclaration>();
        res->name = name;
        return res;
    }
};

}
