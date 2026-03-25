#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

void ASTAssignment::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Assignment");
    w.writeString("column_name", column_name);
    w.writeChildren(children);
}

void ASTAssignment::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    column_name = r.getString("column_name");
    children = r.readChildren();
}

}
