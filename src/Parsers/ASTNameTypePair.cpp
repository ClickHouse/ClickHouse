#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTDataType.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTNameTypePair::clone() const
{
    auto res = make_intrusive<ASTNameTypePair>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}


void ASTNameTypePair::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "NameTypePair");
    w.writeString("name", name);
    w.writeChild("name_type", type);
}

void ASTNameTypePair::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    /// `ParserNameTypePair` produces a non-empty identifier followed by `ParserDataType`, and
    /// `DataTypeTuple`/`DataTypeNested` read `name` and pass `type` to `DataTypeFactory`. Require a
    /// non-empty name and a data-type `name_type` (`dynamic_cast`, since the type may be an
    /// `ASTDataType`/`ASTEnumDataType`/`ASTTupleDataType`) so malformed `clickhouse_json` fails closed.
    if (!r.has("name"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'name' during AST JSON deserialization");
    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for `NameTypePair` during AST JSON deserialization");

    type = r.readChild("name_type");
    if (!type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'name_type' during AST JSON deserialization");
    if (!dynamic_cast<const ASTDataType *>(type.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`NameTypePair` 'name_type' must be a data type during AST JSON deserialization");
    children.push_back(type);
}

void ASTNameTypePair::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << backQuoteIfNeed(name) << ' ';
    type->format(ostr, settings, state, frame);
}

}
