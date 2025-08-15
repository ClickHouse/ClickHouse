#include <Parsers/Access/ASTPublicSSHKey.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTPublicSSHKey::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << "KEY ";
    settings.ostr << backQuoteIfNeed(key_base64) << ' ';
    settings.ostr << "TYPE ";
    settings.ostr << backQuoteIfNeed(type);
}

}
