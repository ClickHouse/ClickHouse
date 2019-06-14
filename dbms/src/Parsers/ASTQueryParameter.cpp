#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << backQuoteIfNeed(name) + ':' + type;
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

}
