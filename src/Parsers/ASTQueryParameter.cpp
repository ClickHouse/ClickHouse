#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.writeSubstitution("{");
    settings.writeProbablyBackQuotedIdentifier(name);
    settings.writeSubstitution(":");
    settings.writeProbablyBackQuotedIdentifier(type);
    settings.writeSubstitution("}");
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

}
