#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>


namespace DB
{

// static
const BlockMissingValues IInputFormat::none;

bool IInputFormat::reset()
{
    return in.reset();
}

}
