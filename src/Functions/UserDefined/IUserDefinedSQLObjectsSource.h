#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{
struct Settings;

enum class UserDefinedSQLObjectType
{
    Function
};

/// Interface for a loader of user-defined sql objects. Implementations: UserDefinedSQLObjectsFromFolder.
class IUserDefinedSQLObjectsSource
{
public:
    virtual ~IUserDefinedSQLObjectsSource() = default;
    virtual void storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & ast, bool replace, const Settings & settings) = 0;
    virtual void removeObject(UserDefinedSQLObjectType object_type, const String & object_name) = 0;
};

}
