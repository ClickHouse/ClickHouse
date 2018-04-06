#include "iostream_debug_helpers.h"
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>

namespace DB
{


std::ostream & operator<<(std::ostream & stream, const Token & what) {
    //stream << "TokenIterator ( index=" << what.index << ") {" << get() << "}";
    stream << "Token (type="<< static_cast<int>(what.type) <<"){"<< std::string{what.begin, what.end} << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Expected & what) {
    stream << "Expected {variants=" 
    //<< what.variants 
    << "; max_parsed_pos=" << what.max_parsed_pos << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TokenIterator & what) {
    //stream << "TokenIterator ( index=" << what.index << ") {" << get() << "}";
    auto copy = what; // avoid const x[] call
    stream << "TokenIterator { "<< copy.get() << "}";
    return stream;
}

}
