#include <Interpreters/LLM/registerModelEntity.h>
#include <Interpreters/LLM/ModelEntityFactory.h>

namespace DB
{

void registerRemoteModelEntity(ModelEntityFactory & factory);

void registerModelEntities()
{
    auto & factory = ModelEntityFactory::instance();
    registerRemoteModelEntity(factory);
}

}
