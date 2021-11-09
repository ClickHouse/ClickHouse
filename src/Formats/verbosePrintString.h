#pragma once


namespace DB
{

class WriteBuffer;
class ReadBuffer;


/** Print string in double quotes and with control characters in "<NAME>" form - for output diagnostic info to user.
  */
void verbosePrintString(const char * begin, const char * end, WriteBuffer & out);

}
