#include <Functions/UserDefined/createUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsLoaderFromDisk.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsLoaderFromZooKeeper.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

std::unique_ptr<IUserDefinedSQLObjectsLoader> createUserDefinedSQLObjectsLoader(const ContextMutablePtr & global_context)
{
    const auto & config = global_context->getConfigRef();
    if (config.has("user_defined_zookeeper_path"))
    {
        if (config.has("user_defined_path"))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'user_defined_path' and 'user_defined_zookeeper_path' must not be both specified in the config");
        }
        return std::make_unique<UserDefinedSQLObjectsLoaderFromZooKeeper>(global_context, config.getString("user_defined_zookeeper_path"));
    }

    String default_path = fs::path{global_context->getPath()} / "user_defined/";
    String path = config.getString("user_defined_path", default_path);
    return std::make_unique<UserDefinedSQLObjectsLoaderFromDisk>(global_context, path);
}

}
