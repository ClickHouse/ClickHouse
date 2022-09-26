#pragma once

#include <Interpreters/Context_fwd.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsSource.h>


namespace DB
{

/// Loads user-defined sql objects from a specified folder.
class UserDefinedSQLObjectsFromFolder : public IUserDefinedSQLObjectsSource
{
public:
    UserDefinedSQLObjectsFromFolder(const ContextPtr & global_context_, const String & dir_path_);

    void storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace, const Settings & settings) override;
    void removeObject(UserDefinedSQLObjectType object_type, const String & object_name) override;

private:
    void createDirectory();
    void loadObjects(const ContextPtr & context);
    void loadUserDefinedObject(const ContextPtr & context, UserDefinedSQLObjectType object_type, std::string_view object_name, const String & file_path);

    String dir_path;
    Poco::Logger * log;
};

}
