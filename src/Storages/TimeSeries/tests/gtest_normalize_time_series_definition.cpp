#include <Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.h>

#include <Common/Exception.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <utility>
#include <vector>

using namespace DB;

namespace DB::Setting
{
    extern const SettingsDefaultTableEngine default_table_engine;
}

namespace DB::ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int THERE_IS_NO_COLUMN;
}

namespace
{
    boost::intrusive_ptr<ASTCreateQuery> parseCreateQuery(const String & query)
    {
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query, /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        return boost::intrusive_ptr<ASTCreateQuery>(&ast->as<ASTCreateQuery &>());
    }

    ASTPtr parseExpression(const String & expression)
    {
        ParserExpression parser;
        return parseQuery(parser, expression, /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }

    ColumnDescription makeColumn(const String & name, const String & type, const String & default_expression = {})
    {
        ColumnDescription column{name, DataTypeFactory::instance().get(type)};
        if (!default_expression.empty())
        {
            column.default_desc.kind = ColumnDefaultKind::Default;
            column.default_desc.expression = parseExpression(default_expression);
        }
        return column;
    }

    ColumnsDescription makeColumns(const std::vector<ColumnDescription> & columns)
    {
        ColumnsDescription result;
        for (const auto & column : columns)
            result.add(column);
        return result;
    }

    /// Normalizes the definition of a new table and returns the normalized definition as text.
    String normalizeNewTable(const String & query, NormalizeTimeSeriesDefinitionParams params = {})
    {
        static const Settings default_query_settings;
        if (!params.query_settings)
            params.query_settings = &default_query_settings;
        auto create_query = parseCreateQuery(query);
        normalizeTimeSeriesDefinitionImpl(*create_query, params);
        return create_query->formatWithSecretsOneLine();
    }

    /// Normalizes the definition of a new table with the specified query-level settings and returns the normalized definition as text.
    String normalizeNewTableWithSettings(const String & query, const Settings & query_settings)
    {
        NormalizeTimeSeriesDefinitionParams params;
        params.query_settings = &query_settings;
        return normalizeNewTable(query, params);
    }

    /// Normalizes the definition of a new table with the clause `AS <old_table>` and returns the normalized definition as text.
    /// `old_table_definition` is the stored definition of the old table, i.e. what the database catalog keeps for it.
    String normalizeNewTableAs(const String & query, const String & old_table_definition, NormalizeTimeSeriesDefinitionParams params = {})
    {
        params.as_create_query = parseCreateQuery(old_table_definition);
        return normalizeNewTable(query, params);
    }

    /// Normalizes the stored definition of an existing table (as on ATTACH) and returns the normalized definition as text.
    String normalizeExistingTable(const String & query)
    {
        auto create_query = parseCreateQuery(query);
        NormalizeTimeSeriesDefinitionParams params;
        params.mode = LoadingStrictnessLevel::ATTACH;
        normalizeTimeSeriesDefinitionImpl(*create_query, params);
        return create_query->formatWithSecretsOneLine();
    }

    /// Extracts the outer columns from a normalized definition, e.g. "`metric_name` String, `tags` Map(String, String), ...".
    String extractOuterColumns(const String & definition)
    {
        String prefix = " (";
        String suffix = ") ENGINE = TimeSeries";
        size_t start = definition.find(prefix);
        size_t end = definition.find(suffix);
        if ((start == String::npos) || (end == String::npos) || (end < start))
            return "";
        start += prefix.size();
        return definition.substr(start, end - start);
    }

    /// Extracts the inner columns of a target from a normalized definition, e.g. `extractInnerColumns(definition, "TAGS")`.
    String extractInnerColumns(const String & definition, const String & target)
    {
        String prefix = target + " INNER COLUMNS (";
        String suffix = ") " + target + " INNER ENGINE";
        size_t start = definition.find(prefix);
        if (start == String::npos)
            return "";
        start += prefix.size();
        size_t end = definition.find(suffix, start);
        if (end == String::npos)
            return "";
        return definition.substr(start, end - start);
    }

    /// Extracts the inner engine of a target from a normalized definition, e.g. `extractInnerEngine(definition, "TAGS")`.
    /// The engine of a target is followed by the clause of the next target or by the end of the definition.
    String extractInnerEngine(const String & definition, const String & target)
    {
        static const std::vector<String> target_clauses
            = {" SAMPLES INNER ", " RECENT SAMPLES INNER ", " TAGS INNER ", " TIME RANGES INNER ", " METRIC FAMILIES INNER ", " METRICS INNER "};

        String prefix = target + " INNER ENGINE = ";
        size_t start = definition.find(prefix);
        if (start == String::npos)
            return "";
        start += prefix.size();

        size_t end = definition.size();
        for (const auto & clause : target_clauses)
            end = std::min(end, definition.find(clause, start));
        return definition.substr(start, end - start);
    }

    int getExceptionCode(const std::function<void()> & function)
    {
        try
        {
            function();
        }
        catch (const Exception & e)
        {
            return e.code();
        }
        return 0;
    }

    String getExceptionMessage(const std::function<void()> & function)
    {
        try
        {
            function();
        }
        catch (const Exception & e)
        {
            return e.message();
        }
        return "";
    }

    /// Replays an owner in memory, without creating or editing server metadata files.
    class InMemoryTimeSeriesReplay
    {
    private:
        ThreadStatus thread_status;

    public:
        InMemoryTimeSeriesReplay()
            : context(Context::createCopy(getContext().context))
            , database_name("test_time_series_replay_" + std::to_string(next_database_id.fetch_add(1)))
            , database(std::make_shared<DatabaseMemory>(database_name, context))
        {
            tryRegisterFunctions();
            tryRegisterAggregateFunctions();
            DatabaseCatalog::instance().attachDatabase(database_name, database);
            context->setCurrentDatabase(database_name);
            context->makeQueryContext();
            context->setCurrentQueryId(database_name);
            query_scope = QueryScope::create(context);
        }

        ~InMemoryTimeSeriesReplay()
        {
            DatabaseCatalog::instance().detachDatabase(context, database_name, false, false);
        }

        std::shared_ptr<StorageTimeSeries> attach(const SettingsChanges & stored_settings, bool create_targets = true)
        {
            const StorageID owner_id{database_name, "ts"};
            auto definition = parseCreateQuery(normalizeNewTable(
                "CREATE TABLE " + database_name + ".ts ENGINE = TimeSeries SETTINGS version = "
                + std::to_string(TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES) + " "
                "SAMPLES ENGINE = Memory "
                "TAGS INNER COLUMNS (id UInt64 DEFAULT 1) TAGS ENGINE = Memory "
                "TIME RANGES ENGINE = Memory METRIC FAMILIES ENGINE = Memory"));

            if (create_targets)
            {
                for (const auto kind : StorageTimeSeries::getTargetKinds())
                {
                    const auto * inner_columns = definition->getTargetInnerColumns(kind);
                    auto columns = InterpreterCreateQuery::getColumnsDescription(
                        *inner_columns->columns, context, LoadingStrictnessLevel::FORCE_ATTACH);
                    const auto target_name = getTimeSeriesInnerTableName(kind, owner_id, TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES);
                    auto target = std::make_shared<StorageMemory>(
                        StorageID{database_name, target_name}, columns, ConstraintsDescription{}, String{}, MemorySettings{});
                    database->attachTable(context, target_name, target, {});
                }
            }

            /// Change only the persisted settings after normalizing a valid definition.
            for (const auto & change : stored_settings)
                definition->storage->settings->changes.setSetting(change.name, change.value);
            definition->attach = true;
            auto owner = std::make_shared<StorageTimeSeries>(
                owner_id, context, LoadingStrictnessLevel::FORCE_ATTACH, false, *definition, ColumnsDescription{}, String{});
            database->attachTable(context, owner_id.table_name, owner, {});
            return owner;
        }

        ContextMutablePtr context;

    private:
        inline static std::atomic<UInt64> next_database_id{0};
        String database_name;
        std::shared_ptr<DatabaseMemory> database;
        QueryScope query_scope;
    };

    void expectStoredSetting(const StorageTimeSeries & owner, const String & name, UInt64 value)
    {
        const auto changes = owner.getStorageSettings()->changes();
        const auto it = std::find_if(changes.begin(), changes.end(), [&](const auto & change) { return change.name == name; });
        ASSERT_NE(it, changes.end());
        EXPECT_EQ(it->value.safeGet<UInt64>(), value);
    }

    void expectInvalidStoredSetting(const std::function<void()> & operation, const String & name, const String & expected_message)
    {
        try
        {
            operation();
            FAIL() << "Runtime access accepted invalid " << name;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INVALID_SETTING_VALUE) << e.message();
            EXPECT_TRUE(e.message().contains(name)) << e.message();
            EXPECT_TRUE(e.message().contains(expected_message)) << e.message();
        }
    }

    SelectQueryInfo makeRuntimeQueryInfo()
    {
        SelectQueryInfo query_info;
        query_info.table_expression_modifiers.emplace();
        query_info.storage_limits = std::make_shared<StorageLimitsList>();
        return query_info;
    }

    void readTimeSeriesColumns(StorageTimeSeries & owner, const Names & columns, const ContextPtr & context, QueryPlan & plan)
    {
        const auto metadata = owner.getInMemoryMetadataPtr(context, false);
        const auto snapshot = owner.getStorageSnapshot(metadata, context);
        auto query_info = makeRuntimeQueryInfo();
        owner.read(plan, columns, snapshot, query_info, context, QueryProcessingStage::FetchColumns, 8192, 1);
    }

    void createSamplesSink(StorageTimeSeries & owner, const ContextPtr & context)
    {
        auto insert = make_intrusive<ASTInsertQuery>();
        insert->table_id = owner.getStorageID();
        auto columns = make_intrusive<ASTExpressionList>();
        for (const auto * name : {"metric_name", "tags", "samples"})
            columns->children.emplace_back(make_intrusive<ASTIdentifier>(name));
        insert->columns = columns;
        const auto metadata = owner.getInMemoryMetadataPtr(context, false);
        auto sink = owner.write(insert, metadata, context, false);
        std::static_pointer_cast<TimeSeriesSink>(sink)->onFinish();
    }

    /// The generated columns and engines of the inner tables, with separate row and bucketed sample layouts.
    const String DEFAULT_ID_TYPE = "Tuple(UInt64, LowCardinality(UUID))";
    const String DEFAULT_ID_GENERATOR = "tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))";
    const String DEFAULT_SAMPLES_COLUMNS = "`id` " + DEFAULT_ID_TYPE + ", `timestamp` DateTime64(3) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))";
    const String DEFAULT_SAMPLES_ENGINE = "MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768";
    const String LEGACY_RECENT_SAMPLES_ENGINE = "MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) "
        "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1";
    const String DEFAULT_RECENT_SAMPLES_ENGINE = "MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp, 'UTC'), toIntervalHour(5)) ORDER BY (id, timestamp) "
        "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1";
    const String DEFAULT_BUCKETED_SAMPLES_COLUMNS = "`id` " + DEFAULT_ID_TYPE
        + ", `samples` SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))) CODEC(ZSTD(3)), "
          "`bucket` DateTime64(3), `min_time` SimpleAggregateFunction(min, DateTime64(3)), `max_time` SimpleAggregateFunction(max, DateTime64(3))";
    const String DEFAULT_BUCKETED_SAMPLES_ENGINE
        = "AggregatingMergeTree PARTITION BY toYYYYMM(toTimeZone(bucket, 'UTC')) ORDER BY (id, bucket) SETTINGS index_granularity = 512, index_granularity_bytes = 524288";
    const String DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE
        = "AggregatingMergeTree PARTITION BY toStartOfInterval(toTimeZone(bucket, 'UTC'), toIntervalHour(5)) ORDER BY (id, bucket) "
          "TTL toTimeZone(bucket, 'UTC') + toIntervalSecond(346500) SETTINGS index_granularity = 256, index_granularity_bytes = 262144, ttl_only_drop_parts = 1";
    const String DEFAULT_TAGS_INDEX = ", INDEX tags_idx tags TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 100000000";
    const String DEFAULT_TAGS_COLUMNS = "`id` " + DEFAULT_ID_TYPE + " DEFAULT " + DEFAULT_ID_GENERATOR
        + ", `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String)" + DEFAULT_TAGS_INDEX;
    const String DEFAULT_TAGS_ENGINE = "ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192";
    const String DEFAULT_TIME_RANGES_COLUMNS = "`id` " + DEFAULT_ID_TYPE
        + ", `min_time` SimpleAggregateFunction(min, DateTime64(3)), `max_time` SimpleAggregateFunction(max, DateTime64(3))";
    const String DEFAULT_TIME_RANGES_ENGINE = "AggregatingMergeTree ORDER BY id";
    const String DEFAULT_METRIC_FAMILIES_COLUMNS = "`metric_family` String, `type` LowCardinality(String), `unit` LowCardinality(String), `help` String";
    const String DEFAULT_METRIC_FAMILIES_ENGINE = "ReplacingMergeTree ORDER BY metric_family";
}


class NormalizeTimeSeriesDefinitionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        /// The types of the `min_time` and `max_time` columns of the time ranges table are `SimpleAggregateFunction(min|max, ...)`.
        tryRegisterAggregateFunctions();
    }
};


TEST_F(NormalizeTimeSeriesDefinitionTest, DefaultDefinition)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries");

    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(DateTime64(3), Float64))")) << definition;
    EXPECT_TRUE(definition.contains("version = 11")) << definition;
    EXPECT_TRUE(definition.contains("recent_samples_ttl_seconds = 345600")) << definition;
    EXPECT_TRUE(definition.contains("samples_bucket_step_seconds = 3600")) << definition;
    EXPECT_TRUE(definition.contains("recent_samples_bucket_step_seconds = 900")) << definition;

    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), DEFAULT_BUCKETED_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), DEFAULT_BUCKETED_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), DEFAULT_TAGS_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_BUCKETED_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"), DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_ENGINE);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, Version8DefinitionKeepsRowSamples)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8");

    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(DateTime64(3), Float64))")) << definition;
    EXPECT_TRUE(definition.contains("version = 8")) << definition;
    EXPECT_FALSE(definition.contains("samples_bucket_step_seconds")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, Version9DefinitionKeepsRowSamples)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 9");

    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);
    EXPECT_FALSE(definition.contains("samples_bucket_step_seconds")) << definition;

    auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.ts ENGINE = TimeSeries SETTINGS version = 9", definition);
    EXPECT_EQ(extractInnerColumns(copy, "SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(copy, "RECENT SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, Version10DefinitionKeepsRowSamples)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10");

    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), DEFAULT_SAMPLES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);
    EXPECT_FALSE(definition.contains("samples_bucket_step_seconds")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, BucketedRecentSamplesTTLRejectsOverflow)
{
    for (const String ttl : {"18446744073709551615", "9223372036854774908", "9223372036854775806"})
    {
        SCOPED_TRACE(ttl);
        auto normalize = [&]
        {
            normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = " + ttl);
        };
        EXPECT_EQ(getExceptionCode(normalize), ErrorCodes::INVALID_SETTING_VALUE);
        EXPECT_TRUE(getExceptionMessage(normalize).contains("must not exceed 9223372036854775807"))
            << getExceptionMessage(normalize);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, BucketedRecentSamplesTTLAcceptsIntervalBoundary)
{
    /// This checks the generated interval, not whether adding it to a timestamp is representable.
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 9223372036854774907");
    EXPECT_TRUE(extractInnerEngine(definition, "RECENT SAMPLES").contains("TTL toTimeZone(bucket, 'UTC') + toIntervalSecond(9223372036854775807)"))
        << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, BucketStepsAreValidatedOnFullAttachButPreservedOnReplay)
{
    const auto stored_definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11");
    for (const String setting : {"samples_bucket_step_seconds", "recent_samples_bucket_step_seconds"})
    {
        const String old_setting = setting + (setting == "samples_bucket_step_seconds" ? " = 3600" : " = 900");
        for (const String value : {"0", "4294967296"})
        {
            SCOPED_TRACE(setting + " = " + value);
            auto definition = stored_definition;
            const auto position = definition.find(old_setting);
            ASSERT_NE(position, String::npos);
            definition.replace(position, old_setting.size(), setting + " = " + value);
            definition.replace(0, 6, "ATTACH");

            NormalizeTimeSeriesDefinitionParams params;
            params.mode = LoadingStrictnessLevel::ATTACH;
            auto attach = [&] { normalizeNewTable(definition, params); };
            EXPECT_EQ(getExceptionCode(attach), ErrorCodes::INVALID_SETTING_VALUE);
            EXPECT_TRUE(getExceptionMessage(attach).contains(setting)) << getExceptionMessage(attach);
            EXPECT_TRUE(getExceptionMessage(attach).contains("must be in the range [1, 4294967295]")) << getExceptionMessage(attach);

            for (auto mode : {LoadingStrictnessLevel::FORCE_ATTACH, LoadingStrictnessLevel::FORCE_RESTORE})
            {
                params.mode = mode;
                EXPECT_EQ(normalizeNewTable(definition, params), definition);
            }
            params.mode = LoadingStrictnessLevel::ATTACH;
            params.is_restore_from_backup = true;
            EXPECT_EQ(normalizeNewTable(definition, params), definition);
        }
    }

    for (const String value : {"1", "4294967295"})
    {
        SCOPED_TRACE(value);
        auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11, samples_bucket_step_seconds = "
            + value + ", recent_samples_bucket_step_seconds = " + value);
        definition.replace(0, 6, "ATTACH");
        EXPECT_EQ(normalizeExistingTable(definition), definition);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ReplayedBucketStepsAreValidatedOnSamplesReadAndWrite)
{
    for (const String setting : {"samples_bucket_step_seconds", "recent_samples_bucket_step_seconds"})
    {
        for (const UInt64 value : {0ULL, 4294967296ULL})
        {
            SCOPED_TRACE(setting + " = " + std::to_string(value));
            InMemoryTimeSeriesReplay replay;
            std::shared_ptr<StorageTimeSeries> owner;
            ASSERT_NO_THROW(owner = replay.attach({{setting, value}}));
            expectStoredSetting(*owner, setting, value);

            QueryPlan tags_plan;
            ASSERT_NO_THROW(readTimeSeriesColumns(*owner, {"tags"}, replay.context, tags_plan));
            EXPECT_TRUE(tags_plan.isInitialized());

            /// These are ordinary owner access paths, independent of the selector's guard.
            expectInvalidStoredSetting([&]
            {
                QueryPlan plan;
                readTimeSeriesColumns(*owner, {"samples"}, replay.context, plan);
            }, setting, "must be in the range [1, 4294967295]");
            expectInvalidStoredSetting([&] { createSamplesSink(*owner, replay.context); },
                setting, "must be in the range [1, 4294967295]");
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ReplayedValidBucketStepBoundariesAllowSamplesReadAndWrite)
{
    for (const UInt64 value : {1ULL, 4294967295ULL})
    {
        SCOPED_TRACE(value);
        InMemoryTimeSeriesReplay replay;
        std::shared_ptr<StorageTimeSeries> owner;
        ASSERT_NO_THROW(owner = replay.attach({
            {"samples_bucket_step_seconds", value}, {"recent_samples_bucket_step_seconds", value}}));
        expectStoredSetting(*owner, "samples_bucket_step_seconds", value);
        expectStoredSetting(*owner, "recent_samples_bucket_step_seconds", value);

        QueryPlan plan;
        ASSERT_NO_THROW(readTimeSeriesColumns(*owner, {"samples"}, replay.context, plan));
        EXPECT_TRUE(plan.isInitialized());
        EXPECT_NO_THROW(createSamplesSink(*owner, replay.context));
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ReplayedRecentSamplesTTLIsValidatedBeforeEmptySelectorRange)
{
    for (const UInt64 ttl : {345600ULL, 9223372036854775808ULL})
    {
        SCOPED_TRACE(ttl);
        InMemoryTimeSeriesReplay replay;
        std::shared_ptr<StorageTimeSeries> owner;
        /// Missing targets make this specifically the selector's pre-shortcut settings check.
        ASSERT_NO_THROW(owner = replay.attach({{"recent_samples_ttl_seconds", ttl}}, /* create_targets = */ false));
        expectStoredSetting(*owner, "recent_samples_ttl_seconds", ttl);

        StorageTimeSeriesSelector::Configuration config;
        config.time_series_storage_id = owner->getStorageID();
        config.time_series_version = 11;
        config.table_id_type = DataTypeFactory::instance().get("UInt64");
        config.table_timestamp_type = DataTypeFactory::instance().get("DateTime64(3)");
        config.table_value_type = DataTypeFactory::instance().get("Float64");
        config.selector = PrometheusQueryTree{"m"};
        config.time_scale = 6;
        config.min_time = DateTime64{1};
        config.max_time = DateTime64{2};
        const auto columns = makeColumns({
            makeColumn("id", "UInt64"),
            makeColumn("bucket", "DateTime64(3)"),
            makeColumn("time_series", "Array(Tuple(DateTime64(3), Float64))")});
        StorageTimeSeriesSelector selector{StorageID{"", "selector"}, columns, config};
        ASSERT_TRUE(selector.hasEmptyTimeRange());
        const auto metadata = selector.getInMemoryMetadataPtr(replay.context, false);
        const auto snapshot = selector.getStorageSnapshot(metadata, replay.context);
        auto query_info = makeRuntimeQueryInfo();
        QueryPlan plan;
        auto build_plan = [&]
        {
            return selector.buildQueryPlan(plan, {"id", "bucket", "time_series"}, snapshot, query_info, replay.context,
                QueryProcessingStage::FetchColumns, 8192, 1, StorageTimeSeriesSelector::SamplesReadOrder::Unordered);
        };

        if (ttl == 345600)
        {
            ASSERT_TRUE(build_plan());
            ASSERT_TRUE(plan.isInitialized());
            EXPECT_EQ(plan.getRootNode()->step->getName(), "ReadFromPreparedSource");
            EXPECT_TRUE(plan.getRootNode()->children.empty());
        }
        else
        {
            expectInvalidStoredSetting([&] { build_plan(); }, "recent_samples_ttl_seconds",
                "must not exceed 9223372036854775807");
            EXPECT_FALSE(plan.isInitialized());
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsRejectsSourceBucketStepsBeforeOverrides)
{
    const auto stored_definition = normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 11");
    for (const String setting : {"samples_bucket_step_seconds", "recent_samples_bucket_step_seconds"})
    {
        const String old_setting = setting + (setting == "samples_bucket_step_seconds" ? " = 3600" : " = 900");
        for (const String value : {"0", "4294967296"})
        {
            auto source = stored_definition;
            const auto position = source.find(old_setting);
            ASSERT_NE(position, String::npos);
            source.replace(position, old_setting.size(), setting + " = " + value);
            std::vector<String> overrides = {old_setting, setting + " = DEFAULT"};
            if (setting == "recent_samples_bucket_step_seconds")
                overrides.emplace_back("recent_samples_ttl_seconds = 0");

            for (const auto & override_settings : overrides)
            {
                SCOPED_TRACE(setting + " = " + value + "; " + override_settings);
                auto copy = [&] { normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS " + override_settings, source); };
                EXPECT_EQ(getExceptionCode(copy), ErrorCodes::INVALID_SETTING_VALUE);
                EXPECT_TRUE(getExceptionMessage(copy).contains("must be in the range [1, 4294967295]")) << getExceptionMessage(copy);
            }
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsRejectsSourceRecentSamplesTTLOverflowBeforeOverrides)
{
    auto source = normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 11");
    const String ttl_setting = "recent_samples_ttl_seconds = 345600";
    const auto ttl_position = source.find(ttl_setting);
    ASSERT_NE(ttl_position, String::npos);
    source.replace(ttl_position, ttl_setting.size(), "recent_samples_ttl_seconds = 18446744073709551615");

    for (const String overrides : {
             "",
             " SETTINGS recent_samples_ttl_seconds = 345600",
             " SETTINGS recent_samples_ttl_seconds = 0",
             " SETTINGS recent_samples_ttl_seconds = 345600 RECENT SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)"})
    {
        SCOPED_TRACE(overrides);
        auto copy = [&] { normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + overrides, source); };
        EXPECT_EQ(getExceptionCode(copy), ErrorCodes::INVALID_SETTING_VALUE);
        EXPECT_TRUE(getExceptionMessage(copy).contains("must not exceed 9223372036854775807")) << getExceptionMessage(copy);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, FullAttachRejectsRecentSamplesTTLOverflowButReplayPreservesMetadata)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11");
    const String ttl_setting = "recent_samples_ttl_seconds = 345600";
    const auto ttl_position = definition.find(ttl_setting);
    ASSERT_NE(ttl_position, String::npos);
    definition.replace(ttl_position, ttl_setting.size(), "recent_samples_ttl_seconds = 18446744073709551615");
    definition.replace(0, 6, "ATTACH");

    NormalizeTimeSeriesDefinitionParams params;
    params.mode = LoadingStrictnessLevel::ATTACH;
    auto attach = [&] { normalizeNewTable(definition, params); };
    EXPECT_EQ(getExceptionCode(attach), ErrorCodes::INVALID_SETTING_VALUE);

    params.mode = LoadingStrictnessLevel::FORCE_ATTACH;
    EXPECT_EQ(normalizeNewTable(definition, params), definition);
    params.mode = LoadingStrictnessLevel::ATTACH;
    params.is_restore_from_backup = true;
    EXPECT_EQ(normalizeNewTable(definition, params), definition);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ScalarRecentSamplesTTLDoesNotUseBucketedIntervalGuard)
{
    auto source = normalizeNewTable(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 18446744073709551615");
    EXPECT_TRUE(extractInnerEngine(source, "RECENT SAMPLES").contains("toIntervalSecond(18446744073709551615)")) << source;

    for (const String overrides : {
             " SETTINGS recent_samples_ttl_seconds = 0",
             " SETTINGS recent_samples_ttl_seconds = 345600 RECENT SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)"})
    {
        SCOPED_TRACE(overrides);
        EXPECT_NO_THROW(normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + overrides, source));
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ScalarBucketLayoutSettingsAreRejectedOnFullAttachButPreservedOnReplay)
{
    const auto stored_definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 9");
    const String settings_prefix = "SETTINGS ";
    const auto position = stored_definition.find(settings_prefix);
    ASSERT_NE(position, String::npos);
    for (const String setting : {
             "samples_bucket_step_seconds = 3600", "samples_partition_by = 'toYYYYMM(bucket)'",
             "samples_index_granularity_bytes = 524288", "samples_compression_codec = 'ZSTD(3)'",
             "recent_samples_bucket_step_seconds = 900", "recent_samples_index_granularity_bytes = 262144",
             "recent_samples_compression_codec = 'ZSTD(3)'"})
    {
        SCOPED_TRACE(setting);
        auto definition = stored_definition;
        definition.insert(position + settings_prefix.size(), setting + ", ");
        definition.replace(0, 6, "ATTACH");
        NormalizeTimeSeriesDefinitionParams params;
        params.mode = LoadingStrictnessLevel::ATTACH;
        auto attach = [&] { normalizeNewTable(definition, params); };
        EXPECT_EQ(getExceptionCode(attach), ErrorCodes::INVALID_SETTING_VALUE);
        EXPECT_TRUE(getExceptionMessage(attach).contains("Bucket-layout settings require `version` to be at least 11"))
            << getExceptionMessage(attach);

        params.mode = LoadingStrictnessLevel::FORCE_ATTACH;
        EXPECT_EQ(normalizeNewTable(definition, params), definition);
        params.mode = LoadingStrictnessLevel::ATTACH;
        params.is_restore_from_backup = true;
        EXPECT_EQ(normalizeNewTable(definition, params), definition);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsChecksSourceBucketLayoutSettingsWithoutExternalTargets)
{
    for (UInt64 version : {8, 9, 10})
    {
        for (const String target : {"SAMPLES", "RECENT SAMPLES"})
        {
            const String source = "CREATE TABLE db.src (samples Array(Tuple(DateTime64(3), Float64))) ENGINE = TimeSeries SETTINGS version = "
                + std::to_string(version) + ", id_type = 'UInt64' " + target + " db.unavailable";
            const String inner_target = " " + target + " INNER COLUMNS (extra UInt8)";

            /// Stored source types suffice: no source external target metadata is supplied, even for a valid copy.
            for (const String settings : {"", " SETTINGS version = 9", " SETTINGS version = 10", " SETTINGS version = 11"})
            {
                SCOPED_TRACE(source + settings);
                auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + settings + inner_target, source);
                const bool scalar = (settings == " SETTINGS version = 9" || settings == " SETTINGS version = 10");
                EXPECT_EQ(extractInnerEngine(copy, target), target == "SAMPLES"
                    ? (scalar ? DEFAULT_SAMPLES_ENGINE : DEFAULT_BUCKETED_SAMPLES_ENGINE)
                    : (scalar ? DEFAULT_RECENT_SAMPLES_ENGINE : DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE));
                EXPECT_FALSE(copy.contains("db.unavailable")) << copy;
            }

            if (version != 9)
                continue;
            const String setting = (target == "SAMPLES") ? "samples_bucket_step_seconds" : "recent_samples_bucket_step_seconds";
            const String value = (target == "SAMPLES") ? "3600" : "900";
            auto malformed_source = source;
            const auto position = malformed_source.find("SETTINGS ");
            ASSERT_NE(position, String::npos);
            malformed_source.insert(position + String("SETTINGS ").size(), setting + " = " + value + ", ");
            for (const String & settings : {
                     " SETTINGS version = 9, " + setting + " = DEFAULT",
                     " SETTINGS version = 11, " + setting + " = " + value,
                     " SETTINGS " + setting + " = DEFAULT",
                     String(" SETTINGS recent_samples_ttl_seconds = 0")})
            {
                SCOPED_TRACE(malformed_source + settings);
                const String replacement = (target == "RECENT SAMPLES" && settings.contains("ttl_seconds = 0")) ? "" : inner_target;
                auto copy = [&]
                {
                    normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + settings + replacement, malformed_source);
                };
                EXPECT_EQ(getExceptionCode(copy), ErrorCodes::INVALID_SETTING_VALUE);
                EXPECT_TRUE(getExceptionMessage(copy).contains("Bucket-layout settings require `version` to be at least 11"))
                    << getExceptionMessage(copy);
            }
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, LegacyBucketedInnerSamplesAreRejectedBeforeNormalization)
{
    for (UInt64 version : {8, 9, 10})
    {
        for (const String target : {"SAMPLES", "RECENT SAMPLES"})
        {
            const String definition = "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = " + std::to_string(version)
                + " " + target + " INNER COLUMNS (" + DEFAULT_BUCKETED_SAMPLES_COLUMNS + ") "
                + target + " INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)";
            for (const auto mode : {LoadingStrictnessLevel::CREATE, LoadingStrictnessLevel::ATTACH})
            {
                auto query = definition;
                if (mode == LoadingStrictnessLevel::ATTACH)
                    query.replace(0, 6, "ATTACH");
                SCOPED_TRACE(query);
                NormalizeTimeSeriesDefinitionParams params;
                params.mode = mode;
                auto normalize = [&] { normalizeNewTable(query, params); };
                EXPECT_EQ(getExceptionCode(normalize), ErrorCodes::INCORRECT_QUERY);
                EXPECT_TRUE(getExceptionMessage(normalize).contains("bucketed samples require version 11")) << getExceptionMessage(normalize);
            }

            NormalizeTimeSeriesDefinitionParams restore_params;
            restore_params.is_restore_from_backup = true;
            auto restore = [&] { normalizeNewTable(definition, restore_params); };
            EXPECT_EQ(getExceptionCode(restore), ErrorCodes::INCORRECT_QUERY);
            EXPECT_TRUE(getExceptionMessage(restore).contains("bucketed samples require version 11")) << getExceptionMessage(restore);
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsRejectsStoredLegacyBucketedInnerSamples)
{
    for (UInt64 version : {8, 9, 10})
    {
        for (const String target : {"SAMPLES", "RECENT SAMPLES"})
        {
            const String stored_definition = "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(version)
                + " " + target + " INNER COLUMNS (" + DEFAULT_BUCKETED_SAMPLES_COLUMNS + ") "
                + target + " INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)";
            for (const String copy_settings : {"", " SETTINGS version = 9", " SETTINGS version = 10"})
            {
                SCOPED_TRACE(stored_definition + copy_settings);
                auto copy = [&]
                {
                    normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + copy_settings, stored_definition);
                };
                EXPECT_EQ(getExceptionCode(copy), ErrorCodes::INCORRECT_QUERY);
                EXPECT_TRUE(getExceptionMessage(copy).contains("bucketed samples require version 11")) << getExceptionMessage(copy);
            }
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, FullAttachRejectsLegacyBucketedExternalSamples)
{
    const auto bucket_columns = makeColumns({
        makeColumn("id", DEFAULT_ID_TYPE),
        makeColumn("samples", "SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64)))"),
        makeColumn("bucket", "DateTime64(3)"),
        makeColumn("min_time", "SimpleAggregateFunction(min, DateTime64(3))"),
        makeColumn("max_time", "SimpleAggregateFunction(max, DateTime64(3))")});
    for (UInt64 version : {8, 9})
    {
        for (const auto kind : {ViewTarget::Samples, ViewTarget::RecentSamples})
        {
            const String target = (kind == ViewTarget::Samples) ? "SAMPLES" : "RECENT SAMPLES";
            const String definition = "ATTACH TABLE db.ts ENGINE = TimeSeries SETTINGS version = " + std::to_string(version)
                + " " + target + " db.ext_samples";
            SCOPED_TRACE(definition);
            NormalizeTimeSeriesDefinitionParams params;
            params.mode = LoadingStrictnessLevel::ATTACH;
            params.external_target_columns[kind] = bucket_columns;
            auto attach = [&] { normalizeNewTable(definition, params); };
            EXPECT_EQ(getExceptionCode(attach), ErrorCodes::INCORRECT_QUERY);
            EXPECT_TRUE(getExceptionMessage(attach).contains("bucketed samples require version 11")) << getExceptionMessage(attach);

            /// Metadata replay cannot require an external target to have loaded already.
            params.mode = LoadingStrictnessLevel::FORCE_ATTACH;
            params.external_target_columns.clear();
            EXPECT_NO_THROW(normalizeNewTable(definition, params));
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, TimeRangesTarget)
{
    /// The time ranges target is on by default and disabled by an explicit `store_time_ranges = 0`.
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS store_time_ranges = 0");
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), DEFAULT_TAGS_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);

    /// A TIME RANGES declaration can't be used with the target disabled.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS store_time_ranges = 0 TIME RANGES INNER ENGINE = AggregatingMergeTree");
    }), ErrorCodes::INCORRECT_QUERY);

    /// The types declared in the inner columns of the time ranges table are used, the declared engine gets the generated key.
    definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (timestamp DateTime64(6)) "
        "TIME RANGES INNER COLUMNS (id UInt64) TIME RANGES INNER ENGINE = AggregatingMergeTree");
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"),
        "`id` UInt64, `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");
    EXPECT_EQ(extractInnerEngine(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_ENGINE);
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;

    /// The `id` type must be the same in all the inner tables.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64) TIME RANGES INNER COLUMNS (id UUID)");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// An external time ranges table must have the required columns of the right types.
    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::TimeRanges] = makeColumns({
        makeColumn("id", "UInt64"),
        makeColumn("min_time", "SimpleAggregateFunction(min, DateTime64(3))"),
        makeColumn("max_time", "SimpleAggregateFunction(max, DateTime64(3))")});
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TIME RANGES db.ext_time_ranges", params);
    EXPECT_TRUE(definition.contains(" TIME RANGES db.ext_time_ranges")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), "");
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;

    params.external_target_columns[ViewTarget::TimeRanges] = makeColumns({makeColumn("id", "UInt64"), makeColumn("min_time", "DateTime64(3)")});
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TIME RANGES db.ext_time_ranges", params);
    }), ErrorCodes::THERE_IS_NO_COLUMN);

    params.external_target_columns[ViewTarget::TimeRanges] = makeColumns({
        makeColumn("id", "UInt64"), makeColumn("min_time", "DateTime64(3)"), makeColumn("max_time", "String")});
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TIME RANGES db.ext_time_ranges", params);
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// Tables of earlier versions store `min_time` and `max_time` in the tags table and have no time ranges table,
    /// and the settings of one form of storing the time ranges are rejected for the versions using the other form.
    const String tags_columns_of_version_7 = "`id` " + DEFAULT_ID_TYPE + " DEFAULT " + DEFAULT_ID_GENERATOR
        + ", `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), "
        "`min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))" + DEFAULT_TAGS_INDEX;
    const String tags_engine_of_version_7
        = "AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1";
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7");
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_columns_of_version_7);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), tags_engine_of_version_7);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7, store_time_ranges = 0"); }), ErrorCodes::INVALID_SETTING_VALUE);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0"); }), ErrorCodes::INVALID_SETTING_VALUE);

    /// The clause `AS <other_table>` doesn't copy the version, so a copy of a table of version 7 gets the time ranges table,
    /// the columns `min_time` and `max_time` move out of the tags table, and the settings of the old form aren't copied.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 7, filter_by_min_time_and_max_time = 0"));
    EXPECT_FALSE(definition.contains("min_time_and_max_time")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), DEFAULT_TAGS_COLUMNS);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_COLUMNS);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, Version7DefinitionKeepsRowSamples)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7");

    EXPECT_TRUE(definition.contains("version = 7")) << definition;
    EXPECT_FALSE(definition.contains("samples_bucket_step_seconds")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "SAMPLES").contains("`timestamp` DateTime64(3)")) << definition;
    EXPECT_FALSE(extractInnerColumns(definition, "SAMPLES").contains("`bucket`")) << definition;
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), "MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768");
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"),
        "MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) "
        "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DefaultTableEngineChoosesInnerEngineFamily)
{
    Settings query_settings;
    query_settings[Setting::default_table_engine] = DefaultTableEngine::ReplicatedMergeTree;

    auto definition = normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8", query_settings);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("ReplicatedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "RECENT SAMPLES").starts_with("ReplicatedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("ReplicatedReplacingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TIME RANGES").starts_with("ReplicatedAggregatingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "METRIC FAMILIES").starts_with("ReplicatedReplacingMergeTree ")) << definition;

    query_settings[Setting::default_table_engine] = DefaultTableEngine::SharedMergeTree;
    definition = normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8", query_settings);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("SharedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "RECENT SAMPLES").starts_with("SharedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("SharedReplacingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TIME RANGES").starts_with("SharedAggregatingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "METRIC FAMILIES").starts_with("SharedReplacingMergeTree ")) << definition;

    /// A declared inner engine chooses the family of the other inner engines.
    definition = normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES ENGINE = MergeTree", query_settings);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("MergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("ReplacingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TIME RANGES").starts_with("AggregatingMergeTree ")) << definition;

    query_settings[Setting::default_table_engine] = DefaultTableEngine::MergeTree;
    definition = normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES ENGINE = SharedMergeTree", query_settings);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("SharedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("SharedReplacingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TIME RANGES").starts_with("SharedAggregatingMergeTree ")) << definition;

    /// Only the MergeTree families can be the engines of the inner tables.
    query_settings[Setting::default_table_engine] = DefaultTableEngine::Memory;
    EXPECT_EQ(getExceptionCode([&] { normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8", query_settings); }), ErrorCodes::INCORRECT_QUERY);

    /// Without a default engine the inner engines must be declared.
    query_settings[Setting::default_table_engine] = DefaultTableEngine::None;
    EXPECT_EQ(getExceptionCode([&] { normalizeNewTableWithSettings("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8", query_settings); }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, TypesDeclaredInInnerColumns)
{
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32) TAGS INNER COLUMNS (id UInt64)");

    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(DateTime64(6), Float32))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6), `value` Float32");
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float32 CODEC(ALP, ZSTD(3))");
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"),
        "`id` UInt64, `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");

    /// The same type declared in several places must be the same everywhere.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (id UInt64) TAGS INNER COLUMNS (id UUID)");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// Every inner column must have a type: only the names and the types are checked during the normalization.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS INNER COLUMNS (id DEFAULT sipHash64(tags))");
    }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, IdTypeSetting)
{
    /// The setting applies to tables of version 9 or earlier (see TimeSeriesVersion.h).
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_type = 'UInt64'");
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))");
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;

    /// The setting must match the declared type.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_type = 'UInt64' TAGS INNER COLUMNS (id UUID)");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The `id` type must be comparable and non-Nullable.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_type = 'Nullable(UInt64)'");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeclaredIdTypeFromEachCarrier)
{
    const std::vector<String> declarations = {
        "SAMPLES INNER COLUMNS (id UInt64)",
        "RECENT SAMPLES INNER COLUMNS (id UInt64)",
        "TAGS INNER COLUMNS (id UInt64 DEFAULT toUInt64(4294967297))",
        "TIME RANGES INNER COLUMNS (id UInt64)",
        "SETTINGS id_type = 'UInt64'"};

    for (const auto & declaration : declarations)
    {
        SCOPED_TRACE(declaration);
        auto query = parseCreateQuery("CREATE TABLE db.ts ENGINE = TimeSeries " + declaration);
        const auto definition = query->formatWithSecretsOneLine();
        auto id_type = getTimeSeriesDeclaredIdType(*query);
        ASSERT_TRUE(id_type);
        EXPECT_EQ(id_type->getName(), "UInt64");
        EXPECT_EQ(query->formatWithSecretsOneLine(), definition);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeclaredIdTypeMustAgreeAcrossCarriers)
{
    auto query = parseCreateQuery(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10, id_type = 'UInt64' "
        "SAMPLES INNER COLUMNS (id UInt64) RECENT SAMPLES INNER COLUMNS (id UInt64) "
        "TAGS INNER COLUMNS (id UInt64) TIME RANGES INNER COLUMNS (id UInt64)");
    auto id_type = getTimeSeriesDeclaredIdType(*query);
    ASSERT_TRUE(id_type);
    EXPECT_EQ(id_type->getName(), "UInt64");

    for (const auto & target : {"SAMPLES", "RECENT SAMPLES", "TAGS", "TIME RANGES"})
    {
        SCOPED_TRACE(target);
        query = parseCreateQuery("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_type = 'UInt64' "
            + String(target) + " INNER COLUMNS (id UInt32)");
        EXPECT_EQ(getExceptionCode([&] { getTimeSeriesDeclaredIdType(*query); }), ErrorCodes::BAD_TYPE_OF_FIELD);
    }

    query = parseCreateQuery(
        "CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (id UInt64) TAGS INNER COLUMNS (id UInt32)");
    EXPECT_EQ(getExceptionCode([&] { getTimeSeriesDeclaredIdType(*query); }), ErrorCodes::BAD_TYPE_OF_FIELD);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeclaredIdTypeDoesNotInferDefaults)
{
    const std::vector<String> definitions = {
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10",
        "ATTACH TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10, id_generator = 'sipHash64(tags)' "
            "SAMPLES db.ext_samples RECENT SAMPLES db.ext_recent TAGS db.ext_tags "
            "TIME RANGES db.ext_time_ranges METRIC FAMILIES db.ext_metrics",
        "CREATE TABLE db.ts ENGINE = TimeSeries TAGS INNER COLUMNS (metric_name String)"};

    for (const auto & definition : definitions)
    {
        SCOPED_TRACE(definition);
        auto query = parseCreateQuery(definition);
        const auto original = query->formatWithSecretsOneLine();
        EXPECT_FALSE(getTimeSeriesDeclaredIdType(*query));
        EXPECT_EQ(query->formatWithSecretsOneLine(), original);
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, IdGeneratorSettingRecordsIdType)
{
    /// The setting applies to tables of version 9 or earlier (see TimeSeriesVersion.h).
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_generator = 'sipHash64(tags)' TAGS INNER COLUMNS (id UInt64)");
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;

    /// The `id` column of the inner tags table has no DEFAULT expression: the setting generates identifiers.
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, `metric_name` LowCardinality(String), ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExternalTagsTableRecordsIdTypeAndIdGenerator)
{
    /// External target tables can be used by tables of version 9 or earlier only (see TimeSeriesVersion.h).
    auto external_tags_columns = [](const String & id_type, const String & id_default_expression = {})
    {
        return makeColumns({
            makeColumn("id", id_type, id_default_expression),
            makeColumn("metric_name", "LowCardinality(String)"),
            makeColumn("tags", "Map(LowCardinality(String), String)")});
    };

    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::Tags] = external_tags_columns("UInt64");

    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TAGS db.ext_tags", params);
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(definition.contains("db.ext_tags")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), "");
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))");

    /// The DEFAULT expression of the `id` column of the external table is recorded as the generator.
    params.external_target_columns[ViewTarget::Tags] = external_tags_columns("UInt64", "cityHash64(tags)");
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TAGS db.ext_tags", params);
    EXPECT_TRUE(definition.contains("id_generator = 'cityHash64(tags)'")) << definition;

    /// An explicit `id_generator` wins over the DEFAULT expression.
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_generator = 'sipHash64(tags)' TAGS db.ext_tags", params);
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_FALSE(definition.contains("cityHash64")) << definition;

    /// The `id_type` setting must match the type of the `id` column of the external table.
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, id_type = 'UUID' TAGS db.ext_tags", params);
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The external table must have all the required columns.
    params.external_target_columns[ViewTarget::Tags] = makeColumns({makeColumn("id", "UInt64"), makeColumn("metric_name", "String")});
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TAGS db.ext_tags", params);
    }), ErrorCodes::THERE_IS_NO_COLUMN);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExternalTargetTablesDefineTypes)
{
    auto external_tags_columns = [](const String & id_type)
    {
        return makeColumns({
            makeColumn("id", id_type),
            makeColumn("metric_name", "LowCardinality(String)"),
            makeColumn("tags", "Map(LowCardinality(String), String)")});
    };

    /// The `min_time` and `max_time` columns must have the timestamp type of the external samples table, DateTime64(6) here.
    auto external_time_ranges_columns = [](const String & id_type)
    {
        return makeColumns({
            makeColumn("id", id_type),
            makeColumn("min_time", "SimpleAggregateFunction(min, DateTime64(6))"),
            makeColumn("max_time", "SimpleAggregateFunction(max, DateTime64(6))")});
    };

    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::Samples] = makeColumns(
        {makeColumn("id", "UInt64"), makeColumn("timestamp", "DateTime64(6)"), makeColumn("value", "Float32")});
    params.external_target_columns[ViewTarget::Tags] = external_tags_columns("UInt64");
    params.external_target_columns[ViewTarget::TimeRanges] = external_time_ranges_columns("UInt64");
    params.external_target_columns[ViewTarget::MetricFamilies] = makeColumns(
        {makeColumn("metric_family", "String"), makeColumn("type", "String"), makeColumn("unit", "String"), makeColumn("help", "String")});

    const String query = "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags TIME RANGES db.ext_time_ranges METRIC FAMILIES db.ext_metric_families";

    auto definition = normalizeNewTable(query, params);
    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(DateTime64(6), Float32))")) << definition;
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_FALSE(definition.contains("INNER")) << definition;

    /// The types of the external tables must match each other.
    params.external_target_columns[ViewTarget::Tags] = external_tags_columns("UUID");
    EXPECT_EQ(getExceptionCode([&] { normalizeNewTable(query, params); }), ErrorCodes::BAD_TYPE_OF_FIELD);

    params.external_target_columns[ViewTarget::Tags] = external_tags_columns("UInt64");
    params.external_target_columns[ViewTarget::TimeRanges] = external_time_ranges_columns("UUID");
    EXPECT_EQ(getExceptionCode([&] { normalizeNewTable(query, params); }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// Without an external time ranges table the inner one is generated with the types of the external tables.
    params.external_target_columns.erase(ViewTarget::TimeRanges);
    definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, recent_samples_ttl_seconds = 0 SAMPLES db.ext_samples TAGS db.ext_tags METRIC FAMILIES db.ext_metric_families", params);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"),
        "`id` UInt64, `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, MetricsKeywordIsAliasOfMetricFamilies)
{
    /// The "metric families" target was named "metrics" before, the old keyword is still accepted
    /// but a normalized definition is always written with the new keyword.
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries METRICS INNER ENGINE = ReplacingMergeTree");
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_ENGINE);
    EXPECT_FALSE(definition.contains("METRICS")) << definition;

    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries METRICS INNER COLUMNS (extra UInt8)");
    EXPECT_EQ(extractInnerColumns(definition, "METRIC FAMILIES"),
        "`metric_family` String, `type` LowCardinality(String), `unit` LowCardinality(String), `help` String, `extra` UInt8");

    /// An external metric families table can be specified with either keyword.
    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::MetricFamilies] = makeColumns(
        {makeColumn("metric_family", "String"), makeColumn("type", "String"), makeColumn("unit", "String"), makeColumn("help", "String")});
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 METRICS db.ext_metric_families", params);
    EXPECT_TRUE(definition.contains(" METRIC FAMILIES db.ext_metric_families")) << definition;
    EXPECT_FALSE(definition.contains("METRICS")) << definition;
    EXPECT_EQ(definition, normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 METRIC FAMILIES db.ext_metric_families", params));

    /// A table of a version before the rename is written with the old keyword, so an older server can read its definition.
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 3 METRIC FAMILIES INNER ENGINE = ReplacingMergeTree");
    EXPECT_TRUE(definition.contains("version = 3")) << definition;
    EXPECT_EQ(extractInnerEngine(definition, "METRICS"), "ReplacingMergeTree ORDER BY metric_family_name");
    EXPECT_FALSE(definition.contains("METRIC FAMILIES")) << definition;

    definition = normalizeExistingTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 3, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags METRICS db.ext_metric_families");
    EXPECT_TRUE(definition.contains(" METRICS db.ext_metric_families")) << definition;
    EXPECT_FALSE(definition.contains("METRIC FAMILIES")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, OuterColumns)
{
    /// The prealpha columns `id`, `timestamp`, `value` are not allowed in the outer column list.
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (id String) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (timestamp String) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (value Int32) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);

    /// The `samples` column must have type Array(Tuple(timestamp, value)) with a date/time timestamp and a floating-point value.
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (samples String) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (samples Array(Tuple(String, Float64))) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (samples Array(Tuple(DateTime64(3), String))) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The declared element types of `samples` propagate to the generated samples columns.
    auto definition = normalizeNewTable("CREATE TABLE db.ts (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS version = 8");
    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(UInt32, Float32))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` " + DEFAULT_ID_TYPE + ", `timestamp` UInt32 CODEC(Delta, T64, ZSTD(3)), `value` Float32 CODEC(ALP, ZSTD(3))");

    /// The outer columns are an IO interface which stores no data, so the declared ones are replaced with the canonical list.
    definition = normalizeNewTable("CREATE TABLE db.ts (metric_name Int32, tags String) ENGINE = TimeSeries");
    EXPECT_EQ(extractOuterColumns(definition),
        "`metric_name` String, `tags` Map(String, String), `samples` Array(Tuple(DateTime64(3), Float64)), "
        "`metric_family` String, `type` String, `unit` String, `help` String");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, SamplesOuterColumnNameDependsOnVersion)
{
    /// The outer column with samples is named `samples` from version 3 and `time_series` in the earlier versions (see TimeSeriesVersion.h).
    const String samples_column = "`samples` Array(Tuple(DateTime64(3), Float64))";
    const String time_series_column = "`time_series` Array(Tuple(DateTime64(3), Float64))";

    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries");
    EXPECT_TRUE(definition.contains(samples_column)) << definition;
    EXPECT_FALSE(definition.contains("`time_series`")) << definition;

    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 3");
    EXPECT_TRUE(definition.contains(samples_column)) << definition;

    for (UInt64 version : {0, 1, 2})
    {
        definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = " + std::to_string(version));
        EXPECT_TRUE(definition.contains(time_series_column)) << definition;
        EXPECT_FALSE(definition.contains("`samples`")) << definition;
    }

    /// Both names are accepted in a CREATE query whatever the version is: the column is regenerated under the name
    /// of the version and keeps the declared type.
    definition = normalizeNewTable("CREATE TABLE db.ts (time_series Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS version = 8");
    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(UInt32, Float32))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` " + DEFAULT_ID_TYPE + ", `timestamp` UInt32 CODEC(Delta, T64, ZSTD(3)), `value` Float32 CODEC(ALP, ZSTD(3))");

    definition = normalizeNewTable("CREATE TABLE db.ts (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS version = 2");
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(UInt32, Float32))")) << definition;

    /// The stored definition of a table of an earlier version keeps the old name on ATTACH.
    const String stored_definition_of_version_2 = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 2");
    EXPECT_TRUE(stored_definition_of_version_2.contains(time_series_column)) << stored_definition_of_version_2;
    EXPECT_EQ(normalizeExistingTable(stored_definition_of_version_2), stored_definition_of_version_2);

    /// The clause `AS <other_table>` doesn't copy the version, so a copy of a table of an earlier version gets the new name.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", stored_definition_of_version_2);
    EXPECT_TRUE(definition.contains(samples_column)) << definition;
    EXPECT_FALSE(definition.contains("`time_series`")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, MetricFamilyInnerColumnNameDependsOnVersion)
{
    /// The column of the inner metric families table with the name of a metric family is named `metric_family` from version 6
    /// and `metric_family_name` in the earlier versions (see TimeSeriesVersion.h). The default definition is checked in DefaultDefinition.
    const String old_columns = "`metric_family_name` String, `type` LowCardinality(String), `unit` LowCardinality(String), `help` String";

    const String stored_definition_of_version_5 = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5");
    EXPECT_EQ(extractInnerColumns(stored_definition_of_version_5, "METRIC FAMILIES"), old_columns);
    EXPECT_EQ(extractInnerEngine(stored_definition_of_version_5, "METRIC FAMILIES"), "ReplacingMergeTree ORDER BY metric_family_name");
    EXPECT_EQ(normalizeExistingTable(stored_definition_of_version_5), stored_definition_of_version_5);

    /// A declaration under the name used by the other versions is rejected: nothing would write to that column.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries METRIC FAMILIES INNER COLUMNS (metric_family_name LowCardinality(String)) "
            "METRIC FAMILIES ENGINE = ReplacingMergeTree ORDER BY metric_family_name");
    }), ErrorCodes::INCORRECT_QUERY);
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5 METRIC FAMILIES INNER COLUMNS (metric_family LowCardinality(String))");
    }), ErrorCodes::INCORRECT_QUERY);

    /// An external metric families table must name the column the way the version does, the error suggests renaming the column otherwise.
    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::MetricFamilies] = makeColumns({makeColumn("metric_family_name", "String"), makeColumn("type", "String"), makeColumn("unit", "String"), makeColumn("help", "String")});
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5 METRIC FAMILIES db.ext_metrics", params).contains("db.ext_metrics"));
    auto create_with_new_version = [&] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 METRIC FAMILIES db.ext_metrics", params); };
    EXPECT_EQ(getExceptionCode(create_with_new_version), ErrorCodes::THERE_IS_NO_COLUMN);
    EXPECT_TRUE(getExceptionMessage(create_with_new_version).contains("ALTER TABLE db.ext_metrics RENAME COLUMN metric_family_name TO metric_family"))
        << getExceptionMessage(create_with_new_version);

    params.external_target_columns[ViewTarget::MetricFamilies] = makeColumns({makeColumn("metric_family", "String"), makeColumn("type", "String"), makeColumn("unit", "String"), makeColumn("help", "String")});
    auto create_with_old_version = [&] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5 METRIC FAMILIES db.ext_metrics", params); };
    EXPECT_EQ(getExceptionCode(create_with_old_version), ErrorCodes::THERE_IS_NO_COLUMN);
    EXPECT_TRUE(getExceptionMessage(create_with_old_version).contains("ALTER TABLE db.ext_metrics RENAME COLUMN metric_family TO metric_family_name"))
        << getExceptionMessage(create_with_old_version);

    /// The clause `AS <other_table>` doesn't copy a customized column and the engine keys under the name of the other versions:
    /// they are generated again for the version of the new table, while the other customized parts are copied.
    auto definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 5 METRIC FAMILIES INNER COLUMNS (metric_family_name LowCardinality(String)) "
            "METRIC FAMILIES ENGINE = ReplacingMergeTree PARTITION BY substring(metric_family_name, 1, 1) ORDER BY (metric_family_name, type) SETTINGS index_granularity = 1024"));
    EXPECT_EQ(extractInnerColumns(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), "ReplacingMergeTree ORDER BY metric_family SETTINGS index_granularity = 1024");

    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 5",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries METRIC FAMILIES INNER COLUMNS (metric_family LowCardinality(String)) "
            "METRIC FAMILIES ENGINE = ReplacingMergeTree ORDER BY (metric_family, type)"));
    EXPECT_EQ(extractInnerColumns(definition, "METRIC FAMILIES"), old_columns);
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), "ReplacingMergeTree ORDER BY metric_family_name");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeclaredEnginesWithoutKeysGetGeneratedKeys)
{
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 SAMPLES ENGINE = MergeTree RECENT SAMPLES ENGINE = MergeTree "
        "TAGS ENGINE = ReplacingMergeTree TIME RANGES ENGINE = AggregatingMergeTree METRIC FAMILIES ENGINE = ReplacingMergeTree");
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"), LEGACY_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_ENGINE);
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_ENGINE);

    /// A declared aggregating engine of the tags table gets the setting allowing the tag columns outside the sorting key.
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS ENGINE = AggregatingMergeTree");
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, VersionSetting)
{
    /// An explicit supported version is accepted, an unknown one is rejected.
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10").contains("version = 10"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11").contains("version = 11"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 9").contains("version = 9"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8").contains("version = 8"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7").contains("version = 7"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 6").contains("version = 6"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5").contains("version = 5"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 4").contains("version = 4"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 3").contains("version = 3"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 2").contains("version = 2"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1").contains("version = 1"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 0").contains("version = 0"));
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 999"); }), ErrorCodes::INVALID_SETTING_VALUE);

    /// The clause `AS <other_table>` doesn't copy the version: a new table gets the latest one.
    auto definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 0"));
    EXPECT_TRUE(definition.contains("version = 11")) << definition;
    EXPECT_FALSE(definition.contains("version = 0")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, RecentSamplesPartitionKeyIsVersioned)
{
    /// The timezone is named only from the version that introduced it, so an earlier version keeps
    /// generating the expression it always generated and stays reproducible.
    EXPECT_EQ(
        extractInnerEngine(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7"), "RECENT SAMPLES"),
        LEGACY_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(
        extractInnerEngine(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8"), "RECENT SAMPLES"),
        LEGACY_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 9"), "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10"), "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerEngine(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries"), "RECENT SAMPLES"), DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE);

    /// A scalar v9 copy gets the UTC partition key. The source's own spelling is what identifies its
    /// key as generated, so it is stripped rather than carried over as if it had been declared.
    auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 9",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 7"));
    EXPECT_TRUE(copy.contains("version = 9")) << copy;
    EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), DEFAULT_RECENT_SAMPLES_ENGINE);

    static constexpr std::string_view legacy_replicated_engine
        = "ReplicatedMergeTree('/clickhouse/tables/{shard}/ts_recent_samples', '{replica}') "
          "PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) "
          "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1";
    static constexpr std::string_view pinned_replicated_engine
        = "ReplicatedMergeTree('/clickhouse/tables/{shard}/ts_recent_samples', '{replica}') "
          "PARTITION BY toStartOfInterval(toDateTime(timestamp, 'UTC'), toIntervalHour(5)) ORDER BY (id, timestamp) "
          "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1";

    /// The engine and its arguments are preserved on copy, while the generated partition
    /// key is recognized, stripped, and regenerated with UTC on the new table.
    auto replicated_src = normalizeNewTable(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 7 "
        "RECENT SAMPLES ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ts_recent_samples', '{replica}')");
    EXPECT_EQ(extractInnerEngine(replicated_src, "RECENT SAMPLES"), legacy_replicated_engine);

    auto replicated_copy = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 9", replicated_src);
    EXPECT_TRUE(replicated_copy.contains("version = 9")) << replicated_copy;
    EXPECT_EQ(extractInnerEngine(replicated_copy, "RECENT SAMPLES"), pinned_replicated_engine);

    /// An explicit custom partition key is preserved on copy even when the engine has arguments.
    auto custom_partition_copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 9",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 7 "
            "RECENT SAMPLES ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ts_recent_samples', '{replica}') "
            "PARTITION BY toYYYYMM(timestamp)"));
    EXPECT_TRUE(extractInnerEngine(custom_partition_copy, "RECENT SAMPLES").contains("PARTITION BY toYYYYMM(timestamp)"))
        << custom_partition_copy;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, NormalizationIsIdempotent)
{
    /// A stored definition is normalized again on ATTACH, and it can be replayed as a new table (e.g. on another replica):
    /// both must keep it unchanged, otherwise the generated parts would be applied twice.
    NormalizeTimeSeriesDefinitionParams params_with_external_tags;
    params_with_external_tags.external_target_columns[ViewTarget::Tags] = makeColumns({
        makeColumn("id", "UInt64", "cityHash64(tags)"),
        makeColumn("metric_name", "LowCardinality(String)"),
        makeColumn("tags", "Map(LowCardinality(String), String)")});

    const std::vector<std::pair<String, NormalizeTimeSeriesDefinitionParams>> definitions =
    {
        {"CREATE TABLE db.ts ENGINE = TimeSeries", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 10", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 11", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 9", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 6", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 5", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 4", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 3", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 2", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 6, tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job'}, store_time_ranges = 0, recent_samples_ttl_seconds = 0", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7, tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8, recent_samples_partition_by = 'toStartOfHour(timestamp)' "
         "SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD(1)), extra UInt8) TAGS INNER COLUMNS (id UInt64) "
         "TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS index_granularity = 1024 "
         "TIME RANGES INNER COLUMNS (extra UInt8) TIME RANGES ENGINE = AggregatingMergeTree ORDER BY id SETTINGS index_granularity = 1024", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 8 TAGS db.ext_tags", params_with_external_tags},
    };

    for (const auto & [query, params] : definitions)
    {
        auto stored_text = normalizeNewTable(query, params);
        EXPECT_EQ(normalizeExistingTable(stored_text), stored_text) << query;
        EXPECT_EQ(normalizeNewTable(stored_text, params), stored_text) << query;
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsCopiesInnerDefinitions)
{
    /// The clause `AS <other_table>` copies the inner columns and engines of the other table, except the parts generated
    /// for the other table: they are generated again for the new table from its settings, while the customized parts are copied.

    /// A customized `min_time` column of the time ranges table is not copied if the new table doesn't store time ranges.
    auto definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_time_ranges = 0",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries TIME RANGES INNER COLUMNS (min_time SimpleAggregateFunction(min, DateTime64(3)) CODEC(ZSTD(1)))"));
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), DEFAULT_TAGS_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);

    /// A customized `min_time` column of the tags table of a table of version 6 is not copied: the new table stores
    /// the time ranges in the time ranges table, which is generated for it.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 6 TAGS INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))) CODEC(ZSTD(1)))"));
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), DEFAULT_TAGS_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_COLUMNS);

    /// `tags_to_columns` written in the query replaces the copied one: the column of a removed tag is not copied,
    /// the column of an added tag is generated.
    definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'instance': 'instance', 'region': 'region'}",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'}"));
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` " + DEFAULT_ID_TYPE + " DEFAULT " + DEFAULT_ID_GENERATOR + ", `metric_name` LowCardinality(String), `instance` String, `region` String, "
        "`tags` Map(LowCardinality(String), String)" + DEFAULT_TAGS_INDEX);

    /// The source customizes the codec of `timestamp` and adds an extra column in the samples table, sets the `id` type,
    /// and adds settings to the tags and time ranges engines; everything else is generated.
    const String src = normalizeNewTable(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD(1)), extra UInt8) "
        "TAGS INNER COLUMNS (id UInt64) TAGS ENGINE = ReplacingMergeTree ORDER BY (metric_name, id) SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0 "
        "TIME RANGES ENGINE = AggregatingMergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0");

    /// The customized parts are copied, the generated parts follow the settings of the new table: a copy pinned to version 7
    /// stores `min_time`/`max_time` in the tags table without aggregation, so the tags engine is ReplacingMergeTree with them
    /// in the sorting key, and keeps its settings. The time ranges table of the source is not copied.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 7, aggregate_min_time_and_max_time = 0", src);
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, ZSTD(1)), `value` Float64 CODEC(ALP, ZSTD(3)), `extra` UInt8");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), "
        "`min_time` Nullable(DateTime64(6)), `max_time` Nullable(DateTime64(6))" + DEFAULT_TAGS_INDEX);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id, min_time, max_time) "
        "SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, allow_nullable_key = 1");
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;

    /// A type declared in the query wins over the type of the other table, the other types are inherited: `value` is
    /// Float32 as declared, `timestamp` is DateTime64(6) as in the source. The declared samples columns replace the copied ones.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (value Float32)", src);
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float32");
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float32 CODEC(ALP, ZSTD(3))");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String)" + DEFAULT_TAGS_INDEX);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0");
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"),
        "`id` UInt64, `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");
    EXPECT_EQ(extractInnerEngine(definition, "TIME RANGES"), "AggregatingMergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0");

    /// The inner columns of the other table are not copied for a target replaced with an external table, the types come
    /// from the external table: `timestamp` is DateTime64(3) in the source and DateTime64(6) in the external table.
    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::Samples] = makeColumns(
        {makeColumn("id", "UInt64"), makeColumn("timestamp", "DateTime64(6)"), makeColumn("value", "Float64")});
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 8 SAMPLES db.ext_data",
        normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 8 SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(ZSTD(5)))"), params);
    EXPECT_FALSE(definition.contains("ext_data SAMPLES INNER COLUMNS")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsMergesSettings)
{
    /// The clause `AS <other_table>` copies the settings of the other table. A `SETTINGS` clause written in the query
    /// doesn't replace them: the settings are merged by name, so a written setting overrides the copied one, a written
    /// setting the other table doesn't have is added, and the rest of the other table's settings are still copied.
    const String src = normalizeNewTable(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job'}, store_time_ranges = 0");

    const String tags_with_job = "`id` " + DEFAULT_ID_TYPE + " DEFAULT " + DEFAULT_ID_GENERATOR
        + ", `metric_name` LowCardinality(String), `job` String, `tags` Map(LowCardinality(String), String)" + DEFAULT_TAGS_INDEX;

    /// Without a `SETTINGS` clause: `job` comes from the copied `tags_to_columns`,
    /// and there is no time ranges table because the copied `store_time_ranges` is 0.
    auto definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", src);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_with_job);
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;

    /// The written `store_time_ranges` overrides the copied one so the time ranges table appears,
    /// the written `tags_index_granularity` is added, and `tags_to_columns` is still copied.
    definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_time_ranges = 1, tags_index_granularity = 4096", src);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_with_job);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_COLUMNS);
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").contains("index_granularity = 4096")) << definition;

    /// A setting written as `name = DEFAULT` is a mention of that setting too, so the value of the other table is not
    /// copied for it: the setting gets its default value and the time ranges table appears, while `tags_to_columns` is still copied.
    definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS tags_index_granularity = 4096, store_time_ranges = DEFAULT", src);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_with_job);
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"), DEFAULT_TIME_RANGES_COLUMNS);

    /// A written value wins over a reset of the same setting, so there is no time ranges table.
    definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_time_ranges = 0, store_time_ranges = DEFAULT", src);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_with_job);
    EXPECT_FALSE(definition.contains("TIME RANGES")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsRegeneratesSamplesSettingsForDestinationVersion)
{
    for (UInt64 version : {8, 9, 10, 11})
    {
        const auto source = normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(version));
        for (const String settings : {"", " SETTINGS version = 9", " SETTINGS version = 10", " SETTINGS version = 11"})
        {
            SCOPED_TRACE(std::to_string(version) + settings);
            const auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + settings, source);
            const bool scalar = (settings == " SETTINGS version = 9" || settings == " SETTINGS version = 10");
            EXPECT_EQ(extractInnerEngine(copy, "SAMPLES"), scalar ? DEFAULT_SAMPLES_ENGINE : DEFAULT_BUCKETED_SAMPLES_ENGINE);
            EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), scalar ? DEFAULT_RECENT_SAMPLES_ENGINE : DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE);
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsDowngradesBucketedSourceToScalarTargets)
{
    for (const String timestamp_type : {"DateTime64(6)", "DateTime", "UInt32"})
    {
        const auto source = normalizeNewTable(
            "CREATE TABLE db.src (samples Array(Tuple(" + timestamp_type + ", Float64))) ENGINE = TimeSeries SETTINGS version = 11");

        for (const UInt64 destination_version : {9, 10})
        {
            SCOPED_TRACE(timestamp_type + " -> version " + std::to_string(destination_version));
            const auto query = "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(destination_version);
            const auto copy = normalizeNewTableAs(query, source);
            const auto samples_columns = extractInnerColumns(copy, "SAMPLES");
            const auto samples_engine = extractInnerEngine(copy, "SAMPLES");
            const auto recent_engine = extractInnerEngine(copy, "RECENT SAMPLES");

            EXPECT_TRUE(samples_columns.contains("`timestamp` " + timestamp_type)) << copy;
            EXPECT_TRUE(samples_engine.contains("ORDER BY (id, timestamp)")) << copy;
            EXPECT_FALSE(samples_engine.contains("bucket")) << copy;
            EXPECT_TRUE(recent_engine.contains("ORDER BY (id, timestamp)")) << copy;
            EXPECT_FALSE(recent_engine.contains("bucket")) << copy;
            EXPECT_TRUE(recent_engine.contains("toDateTime(timestamp, 'UTC')")) << copy;
            EXPECT_TRUE(recent_engine.contains("TTL toDateTime(timestamp) + toIntervalSecond(345600)")) << copy;
        }
    }

    /// Explicit destination partition keys are custom definitions, not generated v11 bucket keys to strip.
    const auto source = normalizeNewTable(
        "CREATE TABLE db.src (samples Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries SETTINGS version = 11");
    for (const UInt64 destination_version : {9, 10})
    {
        const auto query = "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(destination_version)
            + " SAMPLES ENGINE = MergeTree PARTITION BY toYYYYMM(toDateTime(timestamp)) ORDER BY (id, timestamp)"
              " RECENT SAMPLES ENGINE = MergeTree PARTITION BY toStartOfHour(toDateTime(timestamp)) ORDER BY (id, timestamp)";
        const auto copy = normalizeNewTableAs(query, source);
        const auto samples_engine = extractInnerEngine(copy, "SAMPLES");
        const auto recent_engine = extractInnerEngine(copy, "RECENT SAMPLES");

        EXPECT_TRUE(samples_engine.contains("PARTITION BY toYYYYMM(toDateTime(timestamp))")) << copy;
        EXPECT_TRUE(recent_engine.contains("PARTITION BY toStartOfHour(toDateTime(timestamp))")) << copy;
        EXPECT_FALSE(samples_engine.contains("bucket")) << copy;
        EXPECT_FALSE(recent_engine.contains("bucket")) << copy;
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsPreservesCustomBucketedPartitionKeys)
{
    for (const auto & [timestamp_type, bucket_expression] : std::vector<std::pair<String, String>>{
             {"DateTime64(6, 'Europe/Berlin')", "bucket"}, {"UInt32", "toDateTime(bucket)"}})
    {
        SCOPED_TRACE(timestamp_type);
        const String samples_partition = "toYYYYMM(" + bucket_expression + ")";
        const String recent_partition = "toStartOfInterval(" + bucket_expression + ", toIntervalHour(5))";
        const auto source = normalizeNewTable(
            "CREATE TABLE db.src (samples Array(Tuple(" + timestamp_type + ", Float64))) ENGINE = TimeSeries SETTINGS version = 11"
            " SAMPLES ENGINE = AggregatingMergeTree PARTITION BY " + samples_partition
            + " RECENT SAMPLES ENGINE = AggregatingMergeTree PARTITION BY " + recent_partition);
        const auto source_samples_engine = extractInnerEngine(source, "SAMPLES");
        const auto source_recent_engine = extractInnerEngine(source, "RECENT SAMPLES");
        ASSERT_TRUE(source_samples_engine.contains("PARTITION BY " + samples_partition)) << source;
        ASSERT_TRUE(source_recent_engine.contains("PARTITION BY " + recent_partition)) << source;

        const auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 11", source);
        EXPECT_EQ(extractInnerEngine(copy, "SAMPLES"), source_samples_engine) << copy;
        EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), source_recent_engine) << copy;
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsPreservesCustomScalarSamplesSettings)
{
    for (UInt64 version : {8, 9, 10})
    {
        SCOPED_TRACE(version);
        const auto version_setting = " SETTINGS version = " + std::to_string(version);
        const auto source = normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries" + version_setting
            + " SAMPLES ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 512, index_granularity_bytes = 524288"
              " RECENT SAMPLES ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 256, index_granularity_bytes = 262144");
        const auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries" + version_setting, source);
        EXPECT_TRUE(extractInnerEngine(source, "SAMPLES").contains("index_granularity = 512, index_granularity_bytes = 524288")) << source;
        EXPECT_TRUE(extractInnerEngine(source, "RECENT SAMPLES").contains("index_granularity = 256, index_granularity_bytes = 262144")) << source;
        EXPECT_EQ(extractInnerEngine(copy, "SAMPLES"), extractInnerEngine(source, "SAMPLES"));
        EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), extractInnerEngine(source, "RECENT SAMPLES"));
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsMergesSamplesGranularitySettings)
{
    for (UInt64 source_version : {8, 9, 10, 11})
    {
        const auto source = normalizeNewTable("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(source_version)
            + ", samples_index_granularity = 1024, recent_samples_index_granularity = 2048");
        for (UInt64 destination_version : {9, 10, 11})
        {
            SCOPED_TRACE(std::to_string(source_version) + " -> " + std::to_string(destination_version));
            const String query = "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = " + std::to_string(destination_version);
            auto copy = normalizeNewTableAs(query, source);
            EXPECT_TRUE(extractInnerEngine(copy, "SAMPLES").contains("index_granularity = 1024")) << copy;
            EXPECT_TRUE(extractInnerEngine(copy, "RECENT SAMPLES").contains("index_granularity = 2048")) << copy;

            copy = normalizeNewTableAs(query + ", samples_index_granularity = DEFAULT, recent_samples_index_granularity = DEFAULT", source);
            const bool scalar = (destination_version <= 10);
            EXPECT_EQ(extractInnerEngine(copy, "SAMPLES"), scalar ? DEFAULT_SAMPLES_ENGINE : DEFAULT_BUCKETED_SAMPLES_ENGINE);
            EXPECT_EQ(extractInnerEngine(copy, "RECENT SAMPLES"), scalar ? DEFAULT_RECENT_SAMPLES_ENGINE : DEFAULT_BUCKETED_RECENT_SAMPLES_ENGINE);

            copy = normalizeNewTableAs(query + ", samples_index_granularity = 4096, recent_samples_index_granularity = 1024", source);
            EXPECT_TRUE(extractInnerEngine(copy, "SAMPLES").contains("index_granularity = 4096")) << copy;
            EXPECT_TRUE(extractInnerEngine(copy, "RECENT SAMPLES").contains("index_granularity = 1024")) << copy;
        }
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsTableWithExternalTargetTables)
{
    /// The stored definition of a table with external target tables keeps the `id` type in the `id_type` setting,
    /// so the columns of the external tables are not needed to create a table `AS` that table.
    const String src = "CREATE TABLE db.src (`time_series` Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries "
        "SETTINGS id_type = 'UInt64', id_generator = 'sipHash64(tags)', version = 2, recent_samples_ttl_seconds = 345600 "
        "SAMPLES db.ext_samples RECENT SAMPLES db.ext_recent_samples TAGS db.ext_tags METRIC FAMILIES db.ext_metric_families";

    auto definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 8 "
        "SAMPLES INNER COLUMNS (extra UInt8) RECENT SAMPLES INNER COLUMNS (extra UInt8) TAGS INNER COLUMNS (extra UInt8) METRIC FAMILIES INNER COLUMNS (extra UInt8)",
        src);

    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    /// The copy uses version 8, so the column with samples is named `samples` (see SamplesOuterColumnNameDependsOnVersion).
    EXPECT_TRUE(definition.contains("`samples` Array(Tuple(DateTime64(6), Float64))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3)), `extra` UInt8");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64, `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), `extra` UInt8" + DEFAULT_TAGS_INDEX);
    /// The copy uses version 8, so it has the time ranges table (the other table of version 2 has none).
    EXPECT_EQ(extractInnerColumns(definition, "TIME RANGES"),
        "`id` UInt64, `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");
    EXPECT_FALSE(definition.contains("ext_")) << definition;

    /// Version 8 permits external targets, but CREATE AS does not copy another table's target references.
    /// With no targets declared here, it generates local scalar targets and preserves the source types.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 8", src);
    EXPECT_FALSE(definition.contains("ext_")) << definition;
    EXPECT_TRUE(definition.contains("version = 8")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))");
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_SAMPLES_ENGINE);

    /// The external target tables of the other table are not copied: the new table gets generated inner tables instead,
    /// with the types of the other table.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", src);
    EXPECT_FALSE(definition.contains("ext_")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `samples` SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(6), value Float64))) CODEC(ZSTD(3)), "
        "`bucket` DateTime64(6), `min_time` SimpleAggregateFunction(min, DateTime64(6)), `max_time` SimpleAggregateFunction(max, DateTime64(6))");
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), DEFAULT_BUCKETED_SAMPLES_ENGINE);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String)" + DEFAULT_TAGS_INDEX);
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"), DEFAULT_TAGS_ENGINE);
    EXPECT_EQ(extractInnerColumns(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_COLUMNS);
    EXPECT_EQ(extractInnerEngine(definition, "METRIC FAMILIES"), DEFAULT_METRIC_FAMILIES_ENGINE);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsTableWithAnotherIdType)
{
    const String src = "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS id_type = 'UInt64', id_generator = 'cityHash64(tags)', version = 2, recent_samples_ttl_seconds = 345600 "
        "SAMPLES INNER COLUMNS (`id` UInt64) TAGS INNER COLUMNS (`id` UInt64)";

    /// The copy gets the latest version, which has no settings `id_type` and `id_generator` (see TimeSeriesVersion.h):
    /// the `id` type is inherited, the expression generating identifiers is generated for it.
    auto definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", src);
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_FALSE(definition.contains("cityHash64")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;

    /// A copy pinned to a version with the settings copies them if the `id` type stays the same.
    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 8", src);
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'cityHash64(tags)'")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, ")) << definition;

    definition = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID)", src);
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UUID DEFAULT reinterpretAsUUID(sipHash128(tags)), ")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "SAMPLES").starts_with("`id` UUID, ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, EarlierVersionDoesNotRecordIdType)
{
    NormalizeTimeSeriesDefinitionParams params;
    params.external_target_columns[ViewTarget::Tags] = makeColumns({
        makeColumn("id", "UInt64"),
        makeColumn("metric_name", "LowCardinality(String)"),
        makeColumn("tags", "Map(LowCardinality(String), String)"),
        makeColumn("min_time", "Nullable(DateTime64(3))"),
        makeColumn("max_time", "Nullable(DateTime64(3))")});

    /// A table pinned to version 1 is defined the way version 1 did it: an older server must be able to read it.
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1 TAGS db.ext_tags", params);
    EXPECT_TRUE(definition.contains("version = 1")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(Delta, T64, ZSTD(3)), `value` Float64 CODEC(ALP, ZSTD(3))");

    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, id_generator = 'sipHash64(tags)' TAGS INNER COLUMNS (id UInt64)");
    EXPECT_FALSE(definition.contains("id_type")) << definition;

    /// The setting itself is rejected for an earlier version.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, id_type = 'UInt64'");
    }), ErrorCodes::INVALID_SETTING_VALUE);

    /// The `id_type` copied from the other table is dropped if this table is pinned to an earlier version,
    /// while the `id` type is still inherited from it.
    definition = normalizeNewTableAs(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 1 "
        "SAMPLES INNER COLUMNS (extra UInt8) TAGS INNER COLUMNS (extra UInt8) METRIC FAMILIES INNER COLUMNS (extra UInt8)",
        "CREATE TABLE db.src (`time_series` Array(Tuple(DateTime64(3), Float64))) ENGINE = TimeSeries "
        "SETTINGS id_type = 'UInt64', id_generator = 'sipHash64(tags)', version = 2, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags METRIC FAMILIES db.ext_metric_families");
    EXPECT_TRUE(definition.contains("version = 1")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "SAMPLES").starts_with("`id` UInt64, ")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, `metric_name` ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeduplicationCacheSettings)
{
    const std::vector<String> settings = {"metric_families_deduplication_cache_size_bytes", "metric_families_deduplication_cache_expiration_seconds",
                                          "tags_deduplication_cache_size_bytes", "tags_deduplication_cache_expiration_seconds"};

    /// The settings are recorded, rejected for an earlier version, and dropped from a copy pinned to an earlier version.
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS metric_families_deduplication_cache_size_bytes = 10, "
        "metric_families_deduplication_cache_expiration_seconds = 10, tags_deduplication_cache_size_bytes = 10, tags_deduplication_cache_expiration_seconds = 10");
    auto copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 6", definition);
    for (const auto & setting : settings)
    {
        EXPECT_TRUE(definition.contains(setting + " = 10")) << definition;
        EXPECT_FALSE(copy.contains(setting)) << copy;
        EXPECT_EQ(getExceptionCode([&] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 6, store_min_time_and_max_time = 0, " + setting + " = 10"); }), ErrorCodes::INVALID_SETTING_VALUE) << setting;
    }

    /// A copy of the latest version keeps the settings of the tags cache: its tags table doesn't store `min_time` and `max_time`.
    copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", definition);
    for (const auto & setting : settings)
        EXPECT_TRUE(copy.contains(setting + " = 10")) << copy;

    /// In a table of an earlier version the cache of the tags table is useless when every insert changes `min_time` and `max_time`:
    /// enabling it is rejected, an explicit zero is harmless, and its settings are dropped from a copy which stores these columns.
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7, tags_deduplication_cache_size_bytes = 10"); }), ErrorCodes::INVALID_SETTING_VALUE);
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 7, tags_deduplication_cache_size_bytes = 0").contains("tags_deduplication_cache_size_bytes = 0"));
    copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 7", definition);
    EXPECT_FALSE(copy.contains("tags_deduplication_cache")) << copy;
    EXPECT_TRUE(copy.contains("metric_families_deduplication_cache_size_bytes = 10")) << copy;

    /// The settings of the tags cache are kept by a copy which inherits `store_min_time_and_max_time = 0` from the old table,
    /// also when that setting follows another setting removed from the copied ones.
    definition = normalizeNewTable(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 7, filter_by_min_time_and_max_time = 0, store_min_time_and_max_time = 0, "
        "tags_deduplication_cache_size_bytes = 10");
    copy = normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 7", definition);
    EXPECT_TRUE(copy.contains("store_min_time_and_max_time = 0")) << copy;
    EXPECT_TRUE(copy.contains("tags_deduplication_cache_size_bytes = 10")) << copy;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsNonTimeSeriesTable)
{
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTableAs("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", "CREATE TABLE db.src (x UInt8) ENGINE = MergeTree ORDER BY x");
    }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExistingTableNeedsNoParams)
{
    /// On ATTACH the external target tables may not be loaded yet, so they are not read.
    auto definition = normalizeExistingTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags METRIC FAMILIES db.ext_metric_families");
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(3), Float64))")) << definition;
    EXPECT_TRUE(definition.contains("db.ext_tags")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ConvertsDefinitionsOfOlderVersions)
{
    /// A definition without the `version` setting was written before versioning existed: it gets version 0,
    /// and it has no recent samples table.
    auto definition = normalizeExistingTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (`id` UUID, `timestamp` DateTime64(3), `value` Float64)");
    EXPECT_TRUE(definition.contains("version = 0")) << definition;
    EXPECT_TRUE(definition.contains("recent_samples_ttl_seconds = 0")) << definition;

    /// The prealpha version declared the columns of the inner tables as outer columns.
    definition = normalizeExistingTable(
        "CREATE TABLE db.ts (`id` UInt64, `timestamp` DateTime64(6), `value` Float32, `metric_name` LowCardinality(String), "
        "`tags` Map(LowCardinality(String), String), `metric_family_name` String, `type` String, `unit` String, `help` String) "
        "ENGINE = TimeSeries");
    EXPECT_TRUE(definition.contains("version = 0")) << definition;
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(6), Float32))")) << definition;
    EXPECT_TRUE(definition.contains("SAMPLES INNER COLUMNS (`id` UInt64, `timestamp` DateTime64(6), `value` Float32)")) << definition;
    EXPECT_TRUE(definition.contains("TAGS INNER COLUMNS (`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), ")) << definition;

    /// The prealpha form is rejected for a new table.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts (`id` UInt64, `timestamp` DateTime64(3), `value` Float64) ENGINE = TimeSeries");
    }), ErrorCodes::INCORRECT_QUERY);
}
