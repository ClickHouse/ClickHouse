#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

class IUserDefinedSQLObjectsStorage;

std::unique_ptr<IUserDefinedSQLObjectsStorage> createUserDefinedSQLObjectsStorage(const ContextMutablePtr & global_context);

}
