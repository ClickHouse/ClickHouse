#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{

static bool operator==(const AIConfiguration & lhs, const AIConfiguration & rhs)
{
    return lhs.provider == rhs.provider &&
           lhs.api_key == rhs.api_key &&
           lhs.base_url == rhs.base_url &&
           lhs.model == rhs.model &&
           lhs.temperature == rhs.temperature &&
           lhs.max_tokens == rhs.max_tokens &&
           lhs.timeout_seconds == rhs.timeout_seconds &&
           lhs.system_prompt == rhs.system_prompt &&
           lhs.max_steps == rhs.max_steps &&
           lhs.enable_schema_access == rhs.enable_schema_access;
}

bool operator==(const ModelConfiguration & lhs, const ModelConfiguration & rhs)
{
    return lhs.name == rhs.name &&
           lhs.batch_size == rhs.batch_size &&
           lhs.ai_config == rhs.ai_config;
}

ModelConfiguration buildModelConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & key)
{
    auto provider = config.getString("llm_models." + key + ".provider");
    auto model_name = config.getString("llm_models." + key + ".model_name");
    auto api_key = config.getString("llm_models." + key + ".api_key");
    auto base_url = config.getString("llm_models." + key + ".base_url");
    auto batch_size = config.getUInt("llm_models." + key + ".batch_size");
    ModelConfiguration model_config {key, batch_size, provider, api_key, base_url, model_name };
    return model_config;
}

IModelEntity::IModelEntity(const ModelConfiguration & config_)
    : log(getLogger(config_.name))
    , config(config_)
{
}

std::string IModelEntity::systemPrompt()
{
    static constexpr auto system_prompt = "You are a semantic analysis expert. You are helping to analyze strings stored in a database column.";
    return system_prompt;
}


}
