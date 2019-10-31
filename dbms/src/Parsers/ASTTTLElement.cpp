
#include <Columns/Collator.h>
#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>


namespace DB
{

void ASTTTLElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    if (destination_type == TTLDestinationType::DISK)
    {
        settings.ostr << " TO DISK " << quoteString(destination_name);
    }
    else if (destination_type == TTLDestinationType::VOLUME)
    {
        settings.ostr << " TO VOLUME " << quoteString(destination_name);
    }
    else if (destination_type == TTLDestinationType::DELETE)
    {
        settings.ostr << " DELETE";
    }
}

}
