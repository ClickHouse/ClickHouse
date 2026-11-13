#include <Storages/TimeSeries/resolvePrometheusQueryTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Parsers/parseQuery.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace DistributedSetting
{
    extern const DistributedSettingsBool skip_unavailable_shards;
    extern const DistributedSettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNEXPECTED_TABLE_ENGINE;
}

std::optional<PrometheusQueryDistributedTarget> resolvePrometheusQueryTarget(const IStorage & storage, const ContextPtr & context)
{
    if (typeid_cast<const StorageTimeSeries *>(&storage))
        return {};

    /// Checked before the engine or the metadata of the table is revealed.
    context->checkAccess(AccessType::SELECT, storage.getStorageID());

    const auto * distributed = typeid_cast<const StorageDistributed *>(&storage);
    if (!distributed)
    {
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "A prometheus query can be executed on a TimeSeries table or on a Distributed table over TimeSeries tables only, "
            "the engine of table {} is {}",
            storage.getStorageID().getNameForLogs(), storage.getName());
    }

    PrometheusQueryDistributedTarget target;

    /// getClusterName() returns "<remote>" for ENGINE=Remote(), which is not a name we can pass to cluster().
    target.cluster_name = distributed->getClusterName();
    if (target.cluster_name == "<remote>")
        throw Exception(
            ErrorCodes::UNEXPECTED_TABLE_ENGINE,
            "A prometheus query is not supported over table {} because it has no cluster name: "
            "it requires a cluster defined in the server configuration",
            storage.getStorageID().getNameForLogs());

    target.remote_time_series_storage_id.database_name = distributed->getRemoteDatabaseName();
    target.remote_time_series_storage_id.table_name = distributed->getRemoteTableName();
    const auto & declared = distributed->getDistributedSettingsRef();
    target.skip_unavailable_shards = declared[DistributedSetting::skip_unavailable_shards].value;
    target.skip_unavailable_shards_mode = declared[DistributedSetting::skip_unavailable_shards_mode].toString();
    return target;
}

UInt64 outerSamplesVersion(const IStorage & storage, const StorageInMemoryMetadata & metadata)
{
    if (const auto * time_series = typeid_cast<const StorageTimeSeries *>(&storage))
        return time_series->getVersion();
    /// A Distributed table has no version: the samples column it declares is named `samples` since that version.
    return metadata.columns.has(TimeSeriesColumnNames::Samples) ? TimeSeriesVersion::MIN_WITH_SAMPLES_OUTER_COLUMN
                                                                : TimeSeriesVersion::MIN_SUPPORTED;
}

namespace
{
    /// Parsed as the planner parses it: a literal true, which isAlwaysTrue() exempts for a row policy, restricts nothing.
    bool isRestrictiveFilter(const String & filter, const ContextPtr & context)
    {
        if (filter.empty())
            return false;
        const auto & settings = context->getSettingsRef();
        ParserExpression parser;
        const auto ast = parseQuery(
            parser, filter.data(), filter.data() + filter.size(), "additional filter",
            settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        bool value = false;
        return !(tryGetLiteralBool(ast.get(), value) && value);
    }

    bool hasRowPolicy(const StorageID & storage_id, const ContextPtr & context)
    {
        auto row_policy_filter
            = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
        return row_policy_filter && !row_policy_filter->isAlwaysTrue();
    }

    /// Matched by the names the planner matches on: the short name in the current database, or the full name.
    bool hasRestrictiveAdditionalFilter(const StorageID & storage_id, const ContextPtr & context)
    {
        for (const auto & filter_entry : context->getSettingsRef()[Setting::additional_table_filters].value)
        {
            const auto & name_and_filter = filter_entry.safeGet<Tuple>();
            const auto & filtered_table = name_and_filter.at(0).safeGet<String>();
            bool matches = (filtered_table == storage_id.getTableName() && context->getCurrentDatabase() == storage_id.getDatabaseName())
                || filtered_table == storage_id.getFullNameNotQuoted();
            if (matches && isRestrictiveFilter(name_and_filter.at(1).safeGet<String>(), context))
                return true;
        }
        return false;
    }
}

void checkPrometheusQueryDistributedRead(const StorageID & storage_id, const ContextPtr & context)
{
    context->checkAccess(AccessType::SELECT, storage_id);

    if (hasRowPolicy(storage_id, context))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A prometheus query is not supported on Distributed table {} while a row policy applies to it: "
            "the query reads the shard-local TimeSeries tables and the policy would not be applied",
            storage_id.getNameForLogs());

    if (hasRestrictiveAdditionalFilter(storage_id, context))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "A prometheus query is not supported on Distributed table {} with an additional_table_filters entry for it: "
            "the query reads the shard-local TimeSeries tables and the filter would not be applied",
            storage_id.getNameForLogs());
}

void checkTimeSeriesSelectorSource(const StorageID & storage_id, const ContextPtr & context)
{
    if (hasRowPolicy(storage_id, context))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Selecting time series is not supported from TimeSeries table {} while a row policy applies to it: "
            "the selector reads its inner tables and the policy would not be applied",
            storage_id.getNameForLogs());

    if (hasRestrictiveAdditionalFilter(storage_id, context))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Selecting time series is not supported from TimeSeries table {} with an additional_table_filters entry for it: "
            "the selector reads its inner tables and the filter would not be applied",
            storage_id.getNameForLogs());
}

}
