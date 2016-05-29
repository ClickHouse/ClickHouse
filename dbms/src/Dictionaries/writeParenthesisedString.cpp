#include "DB/Dictionaries/writeParenthesisedString.h"

void DB::writeParenthesisedString(const String & s, WriteBuffer & buf)
{
	writeChar('(', buf);
	writeString(s, buf);
	writeChar(')', buf);
}
