#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

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

    void writeJSON(WriteBuffer & out) const override
    {
        JSONObjectWriter w(out, "ForeignKeyDeclaration");
        w.writeString("name", name);
        w.writeChildren(children);
    }

    void readJSON(const Poco::JSON::Object & json) override
    {
        JSONObjectReader r(json);
        name = r.getString("name");
        children = r.readChildren();
    }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTForeignKeyDeclaration>();
        res->name = name;
        return res;
    }
};

}
