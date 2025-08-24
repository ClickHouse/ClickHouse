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
    {
        /// The index info is set in MergeTreeDataSelectExecutor.
        ///
        /// But it only needs to be set if it will be used by some `IsReplacement` function derived from this class in the
        /// payload. Otherwise we could be caching information we don't need.
        /// We don't like to pay for what we don't use ;)
        if (is_replaceable == IsReplaceable::IsReplacement && !context->needsIndexInfo())
            context->setNeedsIndexInfo();
    }

protected:
    ContextPtr context;
};

}
