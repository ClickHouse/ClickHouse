#include <Parsers/ASTQualifiedAsterisk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    const auto & qualifier = children.at(0);
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto & qualifier = children.at(0);
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".*";
    for (ASTs::const_iterator it = children.begin() + 1; it != children.end(); ++it)
    {
        settings.ostr << ' ';
        (*it)->formatImpl(settings, state, frame);
    }
}

}
