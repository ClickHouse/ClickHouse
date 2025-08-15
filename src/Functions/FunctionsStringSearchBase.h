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

    const enum class Info
    {
        None,
        Optimizable,
        Optimized
    } info;

    ContextPtr getContext() const { return context; }

    explicit FunctionsStringSearchBase(ContextPtr context_, Info info_)
        : context(context_)
        , info(info_)
    {}

};

}
