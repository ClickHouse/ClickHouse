#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER
#    include <Common/LRUCache.h>
#    include <Common/HashTable/Hash.h>
#    include <Interpreters/JIT/CHJIT.h>

namespace DB
{

class CompiledExpressionCacheEntry
{
public:
    explicit CompiledExpressionCacheEntry(size_t compiled_expression_size_)
        : compiled_expression_size(compiled_expression_size_)
    {}

    size_t getCompiledExpressionSize() const { return compiled_expression_size; }

    virtual ~CompiledExpressionCacheEntry() {}

private:

    size_t compiled_expression_size = 0;

};

struct CompiledFunctionWeightFunction
{
    size_t operator()(const CompiledExpressionCacheEntry & compiled_function) const
    {
        return compiled_function.getCompiledExpressionSize();
    }
};

class CompiledExpressionCache : public LRUCache<UInt128, CompiledExpressionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>
{
public:
    using Base = LRUCache<UInt128, CompiledExpressionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>;
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
