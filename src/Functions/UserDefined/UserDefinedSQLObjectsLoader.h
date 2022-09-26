#pragma once

#include <Interpreters/Context_fwd.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsSource.h>
#include <Parsers/IAST.h>

#include <boost/noncopyable.hpp>


namespace DB
{

/// Loader of user-defined SQL objects (i.e. SQL functions created with command CREATE FUNCTION).
class UserDefinedSQLObjectsLoader
{
public:
    static UserDefinedSQLObjectsLoader & instance();

    /// Reads all SQL objects from a source. Should be called before storeObject() and removeObject().
    void loadObjects(const ContextPtr & global_context);

    /// Writes a SQL object to the source. This function is invoked by CREATE FUNCTION.
    void storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace, const Settings & settings);

    /// Removes a SQL object from the source. This function is invoked by DROP FUNCTION.
    void removeObject(UserDefinedSQLObjectType object_type, const String & object_name);

private:
    std::unique_ptr<IUserDefinedSQLObjectsSource> source;
};

}
