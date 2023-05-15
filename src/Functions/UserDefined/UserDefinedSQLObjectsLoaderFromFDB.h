#pragma once

#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <MetadataSqlFunctions.pb.h>


namespace DB
{

class MetadataStoreFoundationDB;

class UserDefinedSQLObjectsLoaderFromFDB : public IUserDefinedSQLObjectsLoader
{
public:
    UserDefinedSQLObjectsLoaderFromFDB(const ContextPtr & global_context_, const String & dir_path_);

    void loadObjects() override;

    void reloadObjects() override;

    void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) override;

    bool storeObject(
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        const IAST & create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists) override;

private:
    void createDirectory();
    void loadObjectsImpl();
    void loadUserDefinedObjectFromFDB(ContextPtr context, UserDefinedSQLObjectType object_type, const String & sql);
    ASTPtr tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name);
    ASTPtr tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & file_path, bool check_file_exists);
    String getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const;
    ASTPtr parseObjectData(const String & object_data, UserDefinedSQLObjectType object_type);

    ContextPtr global_context;
    String dir_path;
    Poco::Logger * log;
    std::atomic<bool> objects_loaded = false;
    std::shared_ptr<MetadataStoreFoundationDB> meta_store;
};

}
