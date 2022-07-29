#include <Parsers/ASTQualifiedAsterisk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    const auto & qualifier = children.front();
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto & qualifier = children.front();
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".*";

    /// Format column transformers
    for (auto it = ++children.begin(); it != children.end(); ++it)
    {
        settings.ostr << ' ';
        (*it)->formatImpl(settings, state, frame);
    }
}

}
