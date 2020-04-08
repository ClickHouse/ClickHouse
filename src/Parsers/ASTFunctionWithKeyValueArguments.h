#pragma once

#include <Parsers/IAST.h>
#include <Core/Types.h>

namespace DB
{

/// Pair with name and value in lisp programming langugate style. It contain
/// string as key, but value either can be literal or list of
/// pairs.
class ASTPair : public IAST
{
public:
    /// Name or key of pair
    String first;
    /// Value of pair, which can be also list of pairs
    ASTPtr second;
    /// Value is closed in brackets (HOST '127.0.0.1')
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


/// Function with key-value arguments is a function which arguments consist of
/// pairs (see above). For example:
///                                    ->Pair with list of pairs as value<-
/// SOURCE(USER 'clickhouse' PORT 9000 REPLICA(HOST '127.0.0.1' PRIORITY 1) TABLE 'some_table')
class ASTFunctionWithKeyValueArguments : public IAST
{
public:
    /// Name of function
    String name;
    /// Expression list
    ASTPtr elements;
    /// Has brackets around arguments
    bool has_brackets;

    ASTFunctionWithKeyValueArguments(bool has_brackets_ = true)
        : has_brackets(has_brackets_)
    {
    }

public:
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
