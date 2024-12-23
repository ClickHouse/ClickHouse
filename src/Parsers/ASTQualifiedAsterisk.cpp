#include <Parsers/ASTQualifiedAsterisk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    qualifier->formatImpl(ostr, settings, state, frame);
    ostr << ".*";

    if (transformers)
    {
        transformers->formatImpl(ostr, settings, state, frame);
    }
}

}
