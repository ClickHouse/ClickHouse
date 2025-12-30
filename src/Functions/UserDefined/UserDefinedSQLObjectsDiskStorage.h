#pragma once

#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Loads user-defined sql objects from a specified folder.
class UserDefinedSQLObjectsDiskStorage : public UserDefinedSQLObjectsStorageBase
{
public:
    UserDefinedSQLObjectsDiskStorage(const ContextPtr & global_context_, const String & dir_path_);

    void loadObjects() override;

    void reloadObjects() override;

    void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) override;

private:
    bool storeObjectImpl(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        ASTPtr create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeObjectImpl(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        bool throw_if_not_exists) override;

    void createDirectory();
    void loadObjectsImpl();
    ASTPtr tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name);
    ASTPtr tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & file_path, bool check_file_exists);
    String getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const;

    String dir_path;
    LoggerPtr log;
    std::atomic<bool> objects_loaded = false;
};

}
