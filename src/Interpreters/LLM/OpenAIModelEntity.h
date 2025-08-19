#pragma once
#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{
/// This model comes from the OLLAMA platform.
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
    void buildResult(const nlohmann::json & reply,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t offset,
        size_t size) const;
    nlohmann::json parameters;
};

}
