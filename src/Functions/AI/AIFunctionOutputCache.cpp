#include <Functions/AI/AIFunctionOutputCache.h>

#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool ai_function_enable_repeated_output_cache;
    extern const SettingsUInt64 ai_function_repeated_output_cache_size;
}

AIFunctionOutputCache::AIFunctionOutputCache(size_t max_size_in_bytes)
    : Base(CurrentMetrics::end(), CurrentMetrics::end(), max_size_in_bytes)
{
}

UInt128 AIFunctionOutputCache::hash(std::string_view input)
{
    return sipHash128(input.data(), input.size());
}

AIFunctionOutputCachePtr createAIFunctionOutputCache(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    const UInt64 max_size_in_bytes = settings[Setting::ai_function_repeated_output_cache_size];
    if (!settings[Setting::ai_function_enable_repeated_output_cache] || max_size_in_bytes == 0)
        return nullptr;
    return std::make_shared<AIFunctionOutputCache>(max_size_in_bytes);
}

}
