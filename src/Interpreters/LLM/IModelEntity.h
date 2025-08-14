#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <nlohmann/json.hpp>
#include <Columns/ColumnString.h>
#include <Interpreters/Context_fwd.h>
#include <Common/logger_useful.h>

namespace DB
{
/// In the system, multiple large models from different vendors or self-deployed models can be configured.
/// The model provider platforms include OLLAMA, OpenAI...
class IModelEntity
{
public:
    explicit IModelEntity(const std::string & type_);
    IModelEntity(const IModelEntity & o);
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
    LoggerPtr log;
    String type;
    String name;
    size_t batch_size = 64;
};

}
