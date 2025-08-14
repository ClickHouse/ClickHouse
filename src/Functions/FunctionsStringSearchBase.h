#pragma once

#include <variant>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

class FunctionsStringSearchBase : public IFunction
{
protected:
    ContextPtr context;

public:

    const enum Info
    {
        None,
        Optimizable,
        Optimized
    } info;

    ContextPtr getContext() const { return context; }

    explicit FunctionsStringSearchBase(ContextPtr _context, Info _info)
        : context(_context)
        , info(_info)
    {}

};

}
