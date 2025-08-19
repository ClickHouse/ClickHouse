#include <Interpreters/LLM/ModelEntityFactory.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MODEL_ENTITY;
    extern const int LOGICAL_ERROR;
}
namespace
{
static std::string toLower(const std::string & s)
{
    auto s_ = s;
    std::transform(s_.begin(), s_.end(), s_.begin(), [](unsigned char c) { return std::tolower(c); });
    return s_;
}
}
void ModelEntityFactory::registerModelEntity(const std::string & key, CreatorFn creator_fn)
{
    auto lower_type = toLower(key);
    if (!model_entries.emplace(lower_type, std::move(creator_fn)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ModelEntityFactory: the model '{}' is not unique", key);
}

std::shared_ptr<IModelEntity> ModelEntityFactory::get(const ModelConfiguration & config)
{
    auto key = config.ai_config.provider;
    auto creator_it = model_entries.find(key);
    if (creator_it == model_entries.end())
        throw Exception(ErrorCodes::UNKNOWN_MODEL_ENTITY, "The model entity id {} is not registered.", key);
    return creator_it->second(config);
}

ModelEntityFactory & ModelEntityFactory::instance()
{
    static ModelEntityFactory ret;
    return ret;
}

}
