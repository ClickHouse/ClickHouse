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

void ASTQualifiedAsterisk::formatImpl(const FormattingBuffer & out) const
{
    qualifier->formatImpl(out);
    out.ostr << ".*";

    if (transformers)
    {
        transformers->formatImpl(out);
    }
}

}
