#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateDataTypeQuery.h>

#include <boost/noncopyable.hpp>
#include <atomic>


namespace DB
{

class UserDefinedObjectsOnDisk : private boost::noncopyable
{
public:
    static UserDefinedObjectsOnDisk & instance();

    void loadUserDefinedObjects(ContextPtr context);
    void storeUserDefinedDataType(ContextPtr context, const ASTCreateDataTypeQuery & ast);
    void removeUserDefinedDataType(ContextPtr context, const String & name);

private:
    static void loadUserDefinedObject(ContextPtr context, const String & name, const String & path);
    static void executeCreateTypeQuery(const String & query, ContextPtr context, const String & type_name, const String & file_name);

private:
    std::atomic_int user_defined_objects_count = 0;
};

}
