#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    if (children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Assignment JSON must contain exactly one expression child, got {}", children.size());
}

}
