#include <Parsers/ASTQualifiedAsterisk.h>

namespace DB
{

String ASTQualifiedAsterisk::getColumnName() const
{
    const auto & qualifier = children.at(0);
    return qualifier->getColumnName() + ".*";
}

void ASTQualifiedAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto & qualifier = children.at(0);
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".*";
}

}
