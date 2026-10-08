#pragma once

#include "config.h"

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExpireSnapshotsTypes.h>

namespace DB::Iceberg
{

ExpireSnapshotsOptions parseExpireSnapshotsOptions(const ASTPtr & args, ContextPtr context);

}

#endif
