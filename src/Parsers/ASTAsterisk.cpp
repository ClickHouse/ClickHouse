#include <Parsers/ASTAsterisk.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTAsterisk::clone() const
{
    auto clone = std::make_shared<ASTAsterisk>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTAsterisk::appendColumnName(WriteBuffer & ostr) const { ostr.write('*'); }

void ASTAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << "*";
    for (const auto & child : children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }
}

}
