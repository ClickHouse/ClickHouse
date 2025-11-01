#pragma once
#include <Interpreters/LLM/OpenAIModelEntity.h>

namespace DB
{
class AnthropicModelEntity : public OpenAIModelEntity
{
public:
    explicit AnthropicModelEntity(const ModelConfiguration & config_) : OpenAIModelEntity(config_) {}
    AnthropicModelEntity(const AnthropicModelEntity &) = default;
    ~AnthropicModelEntity() override = default;
    void complete(GenerateContext & generate_context) const override;
};

}
