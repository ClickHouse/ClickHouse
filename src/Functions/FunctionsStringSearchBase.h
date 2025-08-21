#pragma once

#include <variant>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

class FunctionsStringSearchBase : public IFunction
{
public:
    enum class Info
    {
        None,
        Optimizable,
        Optimized
    };

    const Info info;

    ContextPtr getContext() const { return context; }

    FunctionsStringSearchBase(Info info_, ContextPtr context_)
        : info(info_)
        , context(context_)
    {}

protected:
    ContextPtr context;
};

}
