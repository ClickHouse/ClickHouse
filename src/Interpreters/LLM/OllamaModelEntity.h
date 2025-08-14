#pragma once
#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{
/// This model comes from the OLLAMA platform.
class OllamaModelEntity : public IModelEntity
{
public:
    explicit OllamaModelEntity(const std::string & type_) : IModelEntity(type_) {}
    OllamaModelEntity(const OllamaModelEntity &);
    ~OllamaModelEntity() override = default;
    bool configChanged(const Poco::Util::AbstractConfiguration & config, const String & config_name) override;
    void reload(const Poco::Util::AbstractConfiguration & config, const String & config_name) override;
    String getCompletionURL() const override { return completion_endpoint; }

    void processCompletion(ContextPtr context, const nlohmann::json & model, const String & user_prompt,
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        size_t offset,
        size_t size,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const override;
protected:
    nlohmann::json makeRequest(const nlohmann::json & model,
        const String & user_prompt,
        size_t batch_size_) const;
    void parseReply(const nlohmann::json & reply,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t offset,
        size_t size) const;
    String completion_endpoint;
    String api_key;
    String parameters_string;
    nlohmann::json parameters;
};

}
