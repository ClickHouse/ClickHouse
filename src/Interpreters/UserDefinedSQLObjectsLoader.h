#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST.h>

#include <boost/noncopyable.hpp>


namespace DB
{

enum class UserDefinedSQLObjectType
{
    Function
};

class UserDefinedSQLObjectsLoader : private boost::noncopyable
{
public:
    static UserDefinedSQLObjectsLoader & instance();
    UserDefinedSQLObjectsLoader();

    void loadObjects(ContextPtr context);
    void storeObject(ContextPtr context, UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace);
    void removeObject(ContextPtr context, UserDefinedSQLObjectType object_type, const String & object_name);

    /// For ClickHouse local if path is not set we can disable loader.
    void enable(bool enable_persistence);

private:

    void loadUserDefinedObject(ContextPtr context, UserDefinedSQLObjectType object_type, const std::string_view & object_name, const String & file_path);
    Poco::Logger * log;
    bool enable_persistence = true;
};

}
