#include <Interpreters/LLM/IModelEntity.h>

namespace DB
{

IModelEntity::IModelEntity(const std::string & type_)
    : log(getLogger(fmt::format("Model-{}", type_)))
    , type(type_)
{
}

IModelEntity::IModelEntity(const IModelEntity & o)
    : type(o.type)
    , name(o.name)
    , batch_size(o.batch_size)
{
}

}
