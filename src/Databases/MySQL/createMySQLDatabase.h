#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

DatabasePtr createMySQLDatabase(const String & database_name, const String & metadata_path, const ASTStorage * define, Context & context);

}

#endif
