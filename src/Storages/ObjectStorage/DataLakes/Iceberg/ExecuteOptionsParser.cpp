#include "config.h"
#if USE_AVRO

#include <limits>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteOptionsParser.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

ExpireSnapshotsOptions parseExpireSnapshotsOptions(const ASTPtr & args, ContextPtr context)
{
    static constexpr std::string_view cmd = "expire_snapshots";
    ExpireSnapshotsOptions options;

    if (!args)
        return options;

    ASTs all_args = args->children;
    auto first_kv_it = getFirstKeyValueArgument(all_args);
    size_t pos_count = static_cast<size_t>(std::distance(all_args.begin(), first_kv_it));

    if (pos_count > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects at most 1 positional argument, got {}", pos_count);

    if (pos_count == 1)
    {
        auto * lit = all_args[0]->as<ASTLiteral>();
        String timestamp = lit ? lit->value.safeGet<String>() : all_args[0]->getColumnName();
        ReadBufferFromString buf(timestamp);
        time_t expire_time;
        readDateTimeText(expire_time, buf);
        options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
    }

    ASTs kv_args(first_kv_it, all_args.end());
    auto parsed_kv = parseKeyValueArguments(kv_args, context);

    for (const auto & [key, value] : parsed_kv)
    {
        if (key == "expire_before")
        {
            if (options.expire_before_ms.has_value())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "expire_snapshots: 'expire_before' specified both as positional argument and named argument");
            if (value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'expire_before' to be a datetime string");
            String timestamp = value.safeGet<String>();
            ReadBufferFromString buf(timestamp);
            time_t expire_time;
            readDateTimeText(expire_time, buf);
            options.expire_before_ms = static_cast<Int64>(expire_time) * 1000;
        }
        else if (key == "retention_period")
            options.retention_period_ms = fieldToPeriodMs(value, cmd, key);
        else if (key == "retain_last")
        {
            Int64 retain_last = fieldToInt64(value, cmd, key);
            if (retain_last <= 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots expects 'retain_last' to be positive");
            if (retain_last > std::numeric_limits<Int32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "expire_snapshots 'retain_last' is too large: {}", retain_last);
            options.retain_last = static_cast<Int32>(retain_last);
        }
        else if (key == "snapshot_ids")
            options.snapshot_ids = fieldToInt64Array(value, cmd, key);
        else if (key == "dry_run")
            options.dry_run = fieldToBool(value, cmd, key);
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown expire_snapshots argument '{}'. Supported: expire_before, retention_period, retain_last, snapshot_ids, dry_run",
                key);
    }

    if (options.snapshot_ids && (options.retention_period_ms || options.retain_last))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "expire_snapshots argument 'snapshot_ids' cannot be combined with 'retention_period' or 'retain_last'");

    return options;
}

}
}

#endif
