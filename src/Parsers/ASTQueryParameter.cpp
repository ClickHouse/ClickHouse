#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormattingBuffer & out) const
{
    out.writeSubstitution("{");
    out.writeProbablyBackQuotedIdentifier(name);
    out.writeSubstitution(":");
    out.writeProbablyBackQuotedIdentifier(type);
    out.writeSubstitution("}");
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

}
