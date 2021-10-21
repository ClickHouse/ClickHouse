#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST.h>

#include <boost/noncopyable.hpp>


namespace DB
{

enum class UserDefinedObjectType
{
    Function
};

class UserDefinedObjectsLoader : private boost::noncopyable
{
public:
    static UserDefinedObjectsLoader & instance();
    UserDefinedObjectsLoader();

    void loadObjects(ContextPtr context);
    void storeObject(ContextPtr context, UserDefinedObjectType object_type, const String & object_name, const IAST & ast);
    void removeObject(ContextPtr context, UserDefinedObjectType object_type, const String & object_name);

private:

    void loadUserDefinedObject(ContextPtr context, UserDefinedObjectType object_type, const std::string_view & object_name, const String & file_path);
    Poco::Logger * log;
};

}
