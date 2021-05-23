#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

enum class Type : char {
    name, filter, any
};

struct Token {
    Type type;
    StringRef string_value;
};

class Path
{
public:
    Path(ReadBuffer & in_);
    bool pathMatch(StringRef name_ref);
private:
    bool advanceToNextToken(size_t token_index, ReadBuffer & in_);
    StringRef readTokenName(ReadBuffer & buf);
    std::vector<Token> path;
    size_t current_Token;
};
}
