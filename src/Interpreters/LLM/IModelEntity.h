#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <nlohmann/json.hpp>
#include <Columns/ColumnString.h>
#include <Interpreters/Context_fwd.h>
#include <Common/logger_useful.h>
#include <Client/AI/AIConfiguration.h>

namespace DB
{

struct ModelConfiguration
{
    std::string name;
    size_t batch_size;
    AIConfiguration ai_config;

    ModelConfiguration(const std::string & name_,
        size_t batch_size_,
        const std::string & provider_,
        const std::string & api_key_,
        const std::string & base_url_,
        const std::string & model_,
        double temperature_ = 0.2,
        size_t max_tokens_ = 1000,
        size_t timeout_seconds_ = 30,
        const std::string system_prompt_ = {},
        size_t max_steps_ = 5,
        bool enable_schema_access_ = false)
       : name(name_)
       , batch_size(batch_size_)
    {
        ai_config.provider = provider_;
        ai_config.api_key = api_key_;
        ai_config.base_url = base_url_;
        ai_config.model = model_;
        ai_config.temperature = temperature_;
        ai_config.max_tokens = max_tokens_;
        ai_config.timeout_seconds = timeout_seconds_;
        ai_config.system_prompt = system_prompt_;
        ai_config.max_steps = max_steps_;
        ai_config.enable_schema_access = enable_schema_access_;
    }
};

bool operator==(const ModelConfiguration & lhs, const ModelConfiguration & rhs);
ModelConfiguration buildModelConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & name);

struct GenerateContext
{
    ContextPtr context;
    const String model_name;
    const nlohmann::json & model;
    const String & user_prompt;
    const ColumnString::Chars & input_data;
    const ColumnString::Offsets & input_data_offsets;
    size_t offset;
    size_t rows;
    ColumnString::Chars & output_data;
    ColumnString::Offsets & output_data_offsets;
};

struct EmbeddedContext
{
    ContextPtr context;
    const String model_name;
    const nlohmann::json & model;
    const ColumnString::Chars & input_data;
    const ColumnString::Offsets & input_data_offsets;
    size_t offset;
    size_t rows;
    PaddedPODArray<float> & output_data;
    PaddedPODArray<unsigned long> & output_data_offsets;
    int dimensions = 0;
};
/// In the system, multiple large models from different vendors or self-deployed models can be configured.
/// The model provider platforms include OLLAMA, OpenAI...
class IModelEntity
{
public:
    explicit IModelEntity(const ModelConfiguration & config);
    IModelEntity(const IModelEntity & o) = default;
    virtual ~IModelEntity() = default;
    const String & getModelName() const { return config.ai_config.model; }
    virtual bool configChanged(const Poco::Util::AbstractConfiguration & config, const String & config_name) = 0;
    virtual void complete(GenerateContext & generate_context) const = 0;
    virtual void embedding(EmbeddedContext & embedding_context) const = 0;
    size_t getBatchSize() const { return config.batch_size; }
    static std::string systemPrompt();
protected:
    LoggerPtr log;
    ModelConfiguration config;
};

}
