#include "CompiledExpressionCache.h"

#if USE_EMBEDDED_COMPILER

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

void CompiledExpressionCacheFactory::init(size_t cache_size)
{
    if (cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompiledExpressionCache was already initialized");

    cache = std::make_unique<CompiledExpressionCache>(cache_size);
}

CompiledExpressionCache * CompiledExpressionCacheFactory::tryGetCache()
{
    return cache.get();
}

}

#endif
