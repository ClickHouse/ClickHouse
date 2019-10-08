#pragma once

#include <Parsers/IAST.h>
#include <Core/Types.h>

namespace DB
{

/// Pair of values where second can also be list of pairs
class ASTPair : public IAST
{
public:
    String first;
    ASTPtr second;
    bool second_with_brackets;

public:
    ASTPair(bool second_with_brackets_)
        : second_with_brackets(second_with_brackets_)
    {
    }

    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// key-value just a pair "key value" separated by space, where key just a word (USER, PASSWORD, HOST etc) and value is just a literal.
/// KeyValueFunction is a function which arguments consist of either key-value pairs or another KeyValueFunction
/// For example: SOURCE(USER 'clickhouse' PASSWORD 'qwerty123' PORT 9000 REPLICA(HOST '127.0.0.1' PRIORITY 1) TABLE 'some_table')
class ASTFunctionWithKeyValueArguments : public IAST
{
public:
    String name;
    ASTPtr elements;

public:
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
