#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateFunctionQuery.h>

#include <boost/noncopyable.hpp>
#include <atomic>


namespace DB
{

class UserDefinedObjectsOnDisk : private boost::noncopyable
{
public:
    static UserDefinedObjectsOnDisk & instance();

    void loadUserDefinedObjects(ContextMutablePtr context);
    void storeUserDefinedFunction(ContextPtr context, const ASTCreateFunctionQuery & ast);
    static void removeUserDefinedFunction(ContextPtr context, const String & name);

private:
    static void loadUserDefinedObject(ContextMutablePtr context, const String & name, const String & path);
    static void executeCreateFunctionQuery(const String & query, ContextMutablePtr context, const String & file_name);

private:
    std::atomic_int user_defined_objects_count = 0;
};

}
