#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr
        << (settings.hilite ? hilite_substitution : "") << '{'
        << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name)
        << (settings.hilite ? hilite_substitution : "") << ':'
        << (settings.hilite ? hilite_identifier : "") << type
        << (settings.hilite ? hilite_substitution : "") << '}'
        << (settings.hilite ? hilite_none : "");
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

void ASTQueryParameter::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
