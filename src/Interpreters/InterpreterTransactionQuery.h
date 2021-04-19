#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

/// Stub for MySQL compatibility
class InterpreterTransactionQuery : public IInterpreter, WithContext
{
public:
    InterpreterTransactionQuery() {}
    BlockIO execute() override { return {}; }
};

}

