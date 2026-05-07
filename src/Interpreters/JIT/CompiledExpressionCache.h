#pragma once

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <Common/CacheBase.h>
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

    virtual ~CompiledExpressionCacheEntry() = default;

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

class CompiledExpressionCache : public CacheBase<UInt128, CompiledExpressionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>
{
public:
    using Base = CacheBase<UInt128, CompiledExpressionCacheEntry, UInt128Hash, CompiledFunctionWeightFunction>;
    using Base::Base;
};

class CompiledExpressionCacheFactory
{
private:
    std::unique_ptr<CompiledExpressionCache> cache;

public:
    static CompiledExpressionCacheFactory & instance();

    void init(size_t cache_size_in_bytes, size_t cache_size_in_elements);
    CompiledExpressionCache * tryGetCache();
};

}

#endif
