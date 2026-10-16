#include <Parsers/ASTIdentifierTypePair.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTIdentifierTypePair::clone() const
{
    auto res = make_intrusive<ASTIdentifierTypePair>();
    res->children.clear();

    if (identifier)
    {
        res->identifier = identifier->clone();
        res->children.push_back(res->identifier);

    }
    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}


void ASTIdentifierTypePair::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    identifier->format(ostr, settings, state, frame);
    ostr << ' ';
    type->format(ostr, settings, state, frame);
}

void ASTIdentifierTypePair::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "IdentifierTypePair");
    w.writeChild("identifier", identifier);
    w.writeChild("data_type", type);
}

void ASTIdentifierTypePair::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    /// `identifier` and `data_type` are parser-owned: `formatImpl` formats them as an identifier/type
    /// pair. Read `identifier` as an `ASTIdentifier` and require `data_type` to be a data-type node
    /// (`dynamic_cast`, since it may be an `ASTDataType`/`ASTEnumDataType`/`ASTTupleDataType`) so a
    /// parser-impossible pair from malformed `clickhouse_json` is rejected at the boundary.
    identifier = r.readChildOfType<ASTIdentifier>("identifier");
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'identifier' during AST JSON deserialization");
    children.push_back(identifier);
    type = r.readChild("data_type");
    if (!type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'data_type' during AST JSON deserialization");
    if (!dynamic_cast<const ASTDataType *>(type.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`IdentifierTypePair` 'data_type' must be a data type during AST JSON deserialization");
    children.push_back(type);
}

}
