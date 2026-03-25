#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

void ASTForeignKeyDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ForeignKeyDeclaration");
    w.writeString("name", name);
    w.writeChildren(children);
}

void ASTForeignKeyDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    name = r.getString("name");
    children = r.readChildren();
}

}
