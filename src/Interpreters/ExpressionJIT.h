#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER
#    include <Functions/IFunction.h>
#    include <Common/LRUCache.h>
#    include <Common/HashTable/Hash.h>


namespace DB
{

struct CompiledFunction
{
    FunctionBasePtr function;
    size_t compiled_size;
};

struct CompiledFunctionWeightFunction
{
    size_t operator()(const CompiledFunction & compiled_function) const
    {
        return compiled_function.compiled_size;
    }
};

/** This child of LRUCache breaks one of it's invariants: total weight may be changed after insertion.
 * We have to do so, because we don't known real memory consumption of generated LLVM code for every function.
 */
class CompiledExpressionCache : public LRUCache<UInt128, CompiledFunction, UInt128Hash, CompiledFunctionWeightFunction>
{
public:
    using Base = LRUCache<UInt128, CompiledFunction, UInt128Hash, CompiledFunctionWeightFunction>;
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
