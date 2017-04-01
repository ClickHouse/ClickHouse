#pragma once

#include <DB/IO/WriteHelpers.h>


namespace DB
{


void writeParenthesisedString(const String & s, WriteBuffer & buf);


}
