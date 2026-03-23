#include <Parsers/ASTIdentifierTypePair.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

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
    identifier = r.readChild("identifier");
    if (identifier)
        children.push_back(identifier);
    type = r.readChild("data_type");
    if (type)
        children.push_back(type);
}

}
