#include "Interpreters/UserDefinedSQLObjectsLoader.h"
#include "Interpreters/UserDefinedSQLObjectsFromFolder.h"
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

UserDefinedSQLObjectsLoader & UserDefinedSQLObjectsLoader::instance()
{
    static UserDefinedSQLObjectsLoader the_instance;
    return the_instance;
}

void UserDefinedSQLObjectsLoader::loadObjects(const ContextPtr & global_context)
{
    chassert(!source);

    String default_user_defined_path = fs::path{global_context->getPath()} / "user_defined/";
    String user_defined_path = global_context->getConfigRef().getString("user_defined_path", default_user_defined_path);
    source = std::make_unique<UserDefinedSQLObjectsFromFolder>(global_context, user_defined_path);
}

void UserDefinedSQLObjectsLoader::storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace, const Settings & settings)
{
    if (source)
        source->storeObject(object_type, object_name, ast, replace, settings);
}

void UserDefinedSQLObjectsLoader::removeObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    if (source)
        source->removeObject(object_type, object_name);
}

}
