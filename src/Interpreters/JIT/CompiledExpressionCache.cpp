#include <Interpreters/JIT/CompiledExpressionCache.h>

#if USE_EMBEDDED_COMPILER

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric CompiledExpressionCacheBytes;
    extern const Metric CompiledExpressionCacheCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CompiledExpressionCacheFactory & CompiledExpressionCacheFactory::instance()
{
    static CompiledExpressionCacheFactory factory;
    return factory;
}

void CompiledExpressionCacheFactory::init(size_t cache_size_in_bytes, size_t cache_size_in_elements)
{
    if (cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompiledExpressionCache was already initialized");

    cache = std::make_unique<CompiledExpressionCache>(CurrentMetrics::CompiledExpressionCacheBytes, CurrentMetrics::CompiledExpressionCacheCount, cache_size_in_bytes, cache_size_in_elements);
}

CompiledExpressionCache * CompiledExpressionCacheFactory::tryGetCache()
{
    return cache.get();
}

}

#endif
