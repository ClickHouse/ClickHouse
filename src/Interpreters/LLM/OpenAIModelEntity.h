#pragma once
#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{
class OpenAIModelEntity : public IModelEntity
{
public:
    explicit OpenAIModelEntity(const ModelConfiguration & config_) : IModelEntity(config_) {}
    OpenAIModelEntity(const OpenAIModelEntity &) = default;
    ~OpenAIModelEntity() override = default;
    bool configChanged(const Poco::Util::AbstractConfiguration & config, const String & config_name) override;

    void complete(GenerateContext & generate_context) const override;
    void embedding(EmbeddedContext & embedding_context) const override;
protected:
    nlohmann::json parameters;
};

}
