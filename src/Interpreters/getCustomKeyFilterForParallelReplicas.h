#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include "Core/SettingsEnums.h"

namespace DB
{

/// Get AST for filter created from custom_key
/// replica_num is the number of the replica for which we are generating filter starting from 0
ASTPtr getCustomKeyFilterForParallelReplica(
    size_t replicas_count,
    size_t replica_num,
    ASTPtr custom_key_ast,
    ParallelReplicasCustomKeyFilterType filter_type,
    const IStorage & storage,
    const ContextPtr & context);

}
