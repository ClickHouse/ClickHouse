#include <Parsers/ASTAsterisk.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTAsterisk::clone() const
{
    auto clone = std::make_shared<ASTAsterisk>(*this);

    if (expression) { clone->expression = expression->clone(); clone->children.push_back(clone->expression); }

    clone->transformers = transformers->clone();
    clone->children.push_back(clone->transformers);

    return clone;
}

void ASTAsterisk::appendColumnName(WriteBuffer & ostr) const { ostr.write('*'); }

void ASTAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (expression)
    {
        expression->formatImpl(settings, state, frame);
        settings.ostr << ".";
    }

    settings.ostr << "*";

    /// Format column transformers
    for (const auto & child : transformers->children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }
}

}
