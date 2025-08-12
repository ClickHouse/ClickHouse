#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <nlohmann/json.hpp>
#include <Columns/ColumnString.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
/// In the system, multiple large models from different vendors or self-deployed models can be configured.
/// The model provider platforms include OLLAMA, OpenAI...
class IModelEntity
{
public:
    explicit IModelEntity(const std::string & type_) : type(type_) {}
    IModelEntity(const IModelEntity & o) : type(o.type), name(o.name) {}
    virtual ~IModelEntity() = default;
    const String & getName() const { return name; }
    virtual bool configChanged(const Poco::Util::AbstractConfiguration & config, const String & config_name) = 0;
    virtual void reload(const Poco::Util::AbstractConfiguration & config, const String & config_name) = 0;
    virtual String getCompletionURL() const = 0;
    virtual void processCompletion(ContextPtr context, const nlohmann::json & model, const String & user_prompt,
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        size_t offset,
        size_t size,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const = 0;
    size_t getBatchSize() const { return batch_size; }
protected:
    String type;
    String name;
    size_t batch_size = 64;
};
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
