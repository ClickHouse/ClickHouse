#include <Columns/Collator.h>
#include <Parsers/ASTTTLElement.h>


namespace DB
{

void ASTTTLElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    if (destination_type == DestinationType::DISK) {
        settings.ostr << " TO DISK ";
    } else if (destination_type == DestinationType::VOLUME) {
        settings.ostr << " TO VOLUME ";
    } else if (destination_type == DestinationType::DELETE) {
        settings.ostr << " DELETE";
    }

    if (destination_type == DestinationType::DISK || destination_type == DestinationType::VOLUME) {
        WriteBufferFromOwnString destination_name_buf;
        writeQuoted(destination_name, destination_name_buf);
        settings.ostr << destination_name_buf.str();
    }
}

}
