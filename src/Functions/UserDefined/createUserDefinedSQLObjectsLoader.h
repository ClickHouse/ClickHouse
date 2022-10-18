#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
class IUserDefinedSQLObjectsLoader;

std::unique_ptr<IUserDefinedSQLObjectsLoader> createUserDefinedSQLObjectsLoader(const ContextMutablePtr & global_context);

}
