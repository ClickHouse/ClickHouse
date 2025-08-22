#pragma once

#include <Interpreters/Context.h>

namespace DB
{

class FullTextSearchFunctionMixin
{
public:
    enum class IsReplaceable
    {
        No,
        Yes,
        IsReplacement
    };

    const IsReplaceable is_replaceable;

    ContextPtr getContext() const { return context; }

    FullTextSearchFunctionMixin(IsReplaceable is_replaceable_, ContextPtr context_)
        : is_replaceable(is_replaceable_)
        , context(context_)
    {}

protected:
    ContextPtr context;
};

}
