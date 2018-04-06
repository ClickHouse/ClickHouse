#pragma once
#include <iostream>

namespace DB
{
class Token;
std::ostream & operator<<(std::ostream & stream, const Token & what);

class Expected;
std::ostream & operator<<(std::ostream & stream, const Expected & what);

class TokenIterator;
std::ostream & operator<<(std::ostream & stream, const TokenIterator & what);

}

/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <Core/iostream_debug_helpers.h>
