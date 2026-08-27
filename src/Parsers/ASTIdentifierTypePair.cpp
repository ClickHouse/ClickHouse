#include <Parsers/ASTIdentifierTypePair.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTIdentifierTypePair::clone() const
{
    auto res = std::make_shared<ASTIdentifierTypePair>();
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

}
