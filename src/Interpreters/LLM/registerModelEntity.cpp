#include <Interpreters/LLM/registerModelEntity.h>
#include <Interpreters/LLM/ModelEntityFactory.h>

namespace DB
{

void registerOpenAIModelEntity(ModelEntityFactory & factory);
void registerAnthropicModelEntity(ModelEntityFactory & factory);

void registerModelEntities()
{
    auto & factory = ModelEntityFactory::instance();
    registerOpenAIModelEntity(factory);
    registerAnthropicModelEntity(factory);
}

}
