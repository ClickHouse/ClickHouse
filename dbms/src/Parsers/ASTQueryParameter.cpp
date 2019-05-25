#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    String name_type = name + ':' + type;
    settings.ostr << name_type;
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

}
