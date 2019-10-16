#include <Columns/Collator.h>
#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>


namespace DB
{

void ASTTTLElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    if (destination_type == DestinationType::DISK)
    {
        settings.ostr << " TO DISK " << quoteString(destination_name);
    }
    else if (destination_type == DestinationType::VOLUME)
    {
        settings.ostr << " TO VOLUME " << quoteString(destination_name);
    }
    else if (destination_type == DestinationType::DELETE)
    {
        settings.ostr << " DELETE";
    }
}

}
