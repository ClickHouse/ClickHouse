#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

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

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTForeignKeyDeclaration>();
        res->name = name;
        return res;
    }
};

}
