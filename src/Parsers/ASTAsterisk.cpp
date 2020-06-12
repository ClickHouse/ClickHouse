#include <Parsers/ASTAsterisk.h>
#include <IO/WriteBuffer.h>

namespace DB
{

ASTPtr ASTAsterisk::clone() const
{
    auto clone = std::make_shared<ASTAsterisk>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTAsterisk::appendColumnName(WriteBuffer & ostr) const { ostr.write('*'); }

void ASTAsterisk::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << "*";
}

}
