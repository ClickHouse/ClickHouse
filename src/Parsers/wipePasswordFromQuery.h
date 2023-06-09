#pragma once
#include <base/types.h>


namespace DB
{

/// Removes a password or its hash from a query if it's specified there or replaces it with some placeholder.
/// This function is used to prepare a query for storing in logs (we don't want logs to contain sensitive information).
/// The function changes only following types of queries:
/// CREATE/ALTER USER.
String wipePasswordFromQuery(const String & query);

}
