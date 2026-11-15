#include <Functions/UserDefined/createUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsDiskStorage.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsZooKeeperStorage.h>
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

std::unique_ptr<IUserDefinedSQLObjectsStorage> createUserDefinedSQLObjectsStorage(const ContextMutablePtr & global_context)
{
    const String zookeeper_path_key = "user_defined_zookeeper_path";
    const String disk_path_key = "user_defined_path";

    const auto & config = global_context->getConfigRef();
    if (config.has(zookeeper_path_key))
    {
        if (config.has(disk_path_key))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'{}' and '{}' must not be both specified in the config",
                zookeeper_path_key,
                disk_path_key);
        }
        return std::make_unique<UserDefinedSQLObjectsZooKeeperStorage>(global_context, config.getString(zookeeper_path_key));
    }

    String default_path = fs::path{global_context->getPath()} / "user_defined" / "";
    String path = config.getString(disk_path_key, default_path);
    return std::make_unique<UserDefinedSQLObjectsDiskStorage>(global_context, path);
}

}
