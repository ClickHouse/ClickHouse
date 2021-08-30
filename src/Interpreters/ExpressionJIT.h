#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER
#    include <Common/LRUCache.h>
#    include <Common/HashTable/Hash.h>

namespace DB
{

class CompiledFunction;

class CompiledFunctionCacheEntry
{
public:
    CompiledFunctionCacheEntry(std::shared_ptr<CompiledFunction> compiled_function_, size_t compiled_function_size_)
        : compiled_function(std::move(compiled_function_))
        , compiled_function_size(compiled_function_size_)
    {}

    std::shared_ptr<CompiledFunction> getCompiledFunction() const { return compiled_function; }

    size_t getCompiledFunctionSize() const { return compiled_function_size; }

private:
    std::shared_ptr<CompiledFunction> compiled_function;

    size_t compiled_function_size;
};

struct CompiledFunctionWeightFunction
{
    size_t operator()(const CompiledFunctionCacheEntry & compiled_function) const
    {
        return compiled_function.getCompiledFunctionSize();
    }
};

/** This child of LRUCache breaks one of it's invariants: total weight may be changed after insertion.
 * We have to do so, because we don't known real memory consumption of generated LLVM code for every function.
 */
class CompiledExpressionCache : public LRUCache<UInt128, CompiledFunctionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>
{
public:
    using Base = LRUCache<UInt128, CompiledFunctionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>;
    using Base::Base;
};

class CompiledExpressionCacheFactory
{
private:
    std::unique_ptr<CompiledExpressionCache> cache;

public:
    static CompiledExpressionCacheFactory & instance();

    void init(size_t cache_size);
    CompiledExpressionCache * tryGetCache();
};

}

#endif
