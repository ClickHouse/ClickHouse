#include <Parsers/ASTAsterisk.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTAsterisk::clone() const
{
    auto clone = std::make_shared<ASTAsterisk>(*this);
    clone->children.clear();

    if (expression) { clone->expression = expression->clone(); clone->children.push_back(clone->expression); }
    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    return clone;
}

void ASTAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    if (expression)
    {
        expression->appendColumnName(ostr);
        writeCString(".", ostr);
    }

    ostr.write('*');
}

void ASTAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (expression)
    {
        expression->formatImpl(settings, state, frame);
        settings.ostr << ".";
    }

    settings.ostr << "*";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

}
