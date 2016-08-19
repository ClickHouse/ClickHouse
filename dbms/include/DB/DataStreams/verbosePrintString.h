#pragma once

#include <DB/IO/WriteBuffer.h>

namespace DB
{

/** Print string in double quotes and with control characters in "<NAME>" form - for output diagnostic info to user.
  */
void verbosePrintString(BufferBase::Position begin, BufferBase::Position end, WriteBuffer & out);

}
