#pragma once
#include <Poco/Util/AbstractConfiguration.h>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <functional>

namespace DB
{

class IModelEntity;
struct ModelConfiguration;
class Context;
/** Allows to create a ModelEntity by the name and configurations.
  */
class ModelEntityFactory : private boost::noncopyable
{
public:
    static ModelEntityFactory & instance();
    using CreatorFn = std::function<std::shared_ptr<IModelEntity>(const ModelConfiguration &)>;
    std::shared_ptr<IModelEntity> get(const ModelConfiguration &);

    void registerModelEntity(const std::string & id, CreatorFn creator_fn);
private:
    std::unordered_map<std::string, CreatorFn> model_entries;
};

}
