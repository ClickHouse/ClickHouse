#pragma once

#include <Interpreters/Context.h>

namespace DB
{

class FullTextSearchFunctionMixin
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

    FullTextSearchFunctionMixin(Info info_, ContextPtr context_)
        : info(info_)
        , context(context_)
    {}

protected:
    ContextPtr context;
};

}
