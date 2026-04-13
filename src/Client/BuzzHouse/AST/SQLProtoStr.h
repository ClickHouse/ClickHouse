#pragma once

#include <base/types.h>

/// Protobuf generated files give lots of warnings, disable them
#if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Weverything"
#endif

#include "SQLGrammar.pb.h"

#if defined(__clang__)
#    pragma clang diagnostic pop
#endif

namespace BuzzHouse
{

void CreateDatabaseToString(String & ret, const CreateDatabase &);
void CreateTableToString(String & ret, const CreateTable &);
void BackupParamsToString(String & ret, const BackupParams &);
void SQLQueryToString(String & ret, const SQLQuery &);

}
