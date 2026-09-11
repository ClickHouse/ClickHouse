#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>

#include <gtest/gtest.h>

#include <functional>
#include <map>

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
    extern const int LOGICAL_ERROR;
    extern const int THERE_IS_NO_COLUMN;
}

namespace
{
    boost::intrusive_ptr<ASTCreateQuery> parseCreateQuery(const String & query)
    {
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, query, /* max_query_size = */ 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto * create_query = ast->as<ASTCreateQuery>();
        if (!create_query)
            throw std::runtime_error("Not a CREATE query: " + query);
        return boost::intrusive_ptr<ASTCreateQuery>(create_query);
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

    /// The columns of an external samples table.
    ColumnsDescription externalSamplesColumns(const String & id_type = "UUID")
    {
        return makeColumns({makeColumn("id", id_type), makeColumn("timestamp", "DateTime64(3)"), makeColumn("value", "Float64")});
    }

    /// The columns of an external tags table.
    ColumnsDescription externalTagsColumns(const String & id_type = "UUID", const String & id_default_expression = {})
    {
        return makeColumns({
            makeColumn("id", id_type, id_default_expression),
            makeColumn("metric_name", "LowCardinality(String)"),
            makeColumn("tags", "Map(LowCardinality(String), String)"),
            makeColumn("min_time", "Nullable(DateTime64(3))"),
            makeColumn("max_time", "Nullable(DateTime64(3))")});
    }

    /// The columns of an external metrics table.
    ColumnsDescription externalMetricsColumns()
    {
        return makeColumns({
            makeColumn("metric_family_name", "String"),
            makeColumn("type", "String"),
            makeColumn("unit", "String"),
            makeColumn("help", "String")});
    }

    /// Normalizes the definition of a new table and returns the normalized definition as text.
    String normalizeNewTable(const String & query, NormalizeTimeSeriesDefinitionInputs inputs = {})
    {
        static const Settings default_query_settings;
        if (!inputs.query_settings)
            inputs.query_settings = &default_query_settings;
        auto create_query = parseCreateQuery(query);
        normalizeTimeSeriesDefinitionImpl(*create_query, LoadingStrictnessLevel::CREATE, /* is_restore_from_backup = */ false, inputs);
        return create_query->formatWithSecretsOneLine();
    }

    /// Normalizes the stored definition of an existing table (as on ATTACH) and returns the normalized definition as text.
    String normalizeExistingTable(const String & query)
    {
        auto create_query = parseCreateQuery(query);
        normalizeTimeSeriesDefinitionImpl(*create_query, LoadingStrictnessLevel::ATTACH, /* is_restore_from_backup = */ false, /* inputs = */ {});
        return create_query->formatWithSecretsOneLine();
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
    /// The targets are written in the order SAMPLES, RECENT SAMPLES, TAGS, METRICS; the engine of a target is followed by the next target.
    String extractInnerEngine(const String & definition, const String & target)
    {
        static const std::map<String, String> next_targets
            = {{"SAMPLES", " RECENT SAMPLES INNER "}, {"RECENT SAMPLES", " TAGS INNER "}, {"TAGS", " METRICS INNER "}};

        String prefix = target + " INNER ENGINE = ";
        size_t start = definition.find(prefix);
        if (start == String::npos)
            return "";
        start += prefix.size();

        size_t end = definition.size();
        if (auto it = next_targets.find(target); it != next_targets.end())
            end = definition.find(it->second, start);
        if (end == String::npos)
            return "";
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

    const String default_id_type = "Tuple(UInt64, LowCardinality(UUID))";
    const String default_id_generator = "tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))";
}


class NormalizeTimeSeriesDefinitionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        /// The types of the `min_time` and `max_time` columns are `SimpleAggregateFunction(min|max, ...)`.
        tryRegisterAggregateFunctions();
    }
};


TEST_F(NormalizeTimeSeriesDefinitionTest, DefaultDefinition)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries");

    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(3), Float64))")) << definition;
    EXPECT_TRUE(definition.contains("version = 2")) << definition;
    EXPECT_TRUE(definition.contains("recent_samples_ttl_seconds = 345600")) << definition;

    /// The `id` type is declared in the inner columns, so there is no need to record it in the settings.
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;

    String samples_columns = "`id` " + default_id_type + ", `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))";
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), samples_columns);
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), samples_columns);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` " + default_id_type + " DEFAULT " + default_id_generator + ", `metric_name` LowCardinality(String), "
        "`tags` Map(LowCardinality(String), String), `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
        "`max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))");
    EXPECT_EQ(extractInnerColumns(definition, "METRICS"),
        "`metric_family_name` String, `type` LowCardinality(String), `unit` LowCardinality(String), `help` String");

    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), "MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768");
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"),
        "MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) "
        "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1");
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1");
    EXPECT_EQ(extractInnerEngine(definition, "METRICS"), "ReplacingMergeTree ORDER BY metric_family_name");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DefaultTableEngineChoosesInnerEngineFamily)
{
    Settings query_settings;
    query_settings[Setting::default_table_engine] = DefaultTableEngine::ReplicatedMergeTree;
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.query_settings = &query_settings;

    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries", inputs);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("ReplicatedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "RECENT SAMPLES").starts_with("ReplicatedMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("ReplicatedAggregatingMergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "METRICS").starts_with("ReplicatedReplacingMergeTree ")) << definition;

    /// A declared inner engine chooses the family of the other inner engines.
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES ENGINE = MergeTree", inputs);
    EXPECT_TRUE(extractInnerEngine(definition, "SAMPLES").starts_with("MergeTree ")) << definition;
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").starts_with("AggregatingMergeTree ")) << definition;

    /// Without a default engine the inner engines must be declared.
    query_settings[Setting::default_table_engine] = DefaultTableEngine::None;
    EXPECT_EQ(getExceptionCode([&] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries", inputs); }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, TypesDeclaredInInnerColumns)
{
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32) TAGS INNER COLUMNS (id UInt64)");

    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(6), Float32))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6), `value` Float32");
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), `value` Float32 CODEC(ZSTD(3))");
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").contains("`min_time` SimpleAggregateFunction(min, Nullable(DateTime64(6)))")) << definition;

    /// The same type declared in several places must be the same everywhere.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES INNER COLUMNS (id UInt64) TAGS INNER COLUMNS (id UUID)");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// Every inner column must have a type: only the names and the types are checked during the normalization.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS INNER COLUMNS (id DEFAULT sipHash64(tags))");
    }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, IdTypeSetting)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_type = 'UInt64'");
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))");
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64 DEFAULT sipHash64(tags), ")) << definition;

    /// The setting must match the declared type.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_type = 'UInt64' TAGS INNER COLUMNS (id UUID)");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The `id` type must be comparable and non-Nullable.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_type = 'Nullable(UInt64)'");
    }), ErrorCodes::BAD_TYPE_OF_FIELD);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, IdGeneratorSettingRecordsIdType)
{
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_generator = 'sipHash64(tags)' TAGS INNER COLUMNS (id UInt64)");
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;

    /// The `id` column of the inner tags table has no DEFAULT expression: the setting generates identifiers.
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, `metric_name` LowCardinality(String), ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExternalTagsTableRecordsIdTypeAndIdGenerator)
{
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UInt64");

    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS db.ext_tags", inputs);
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(definition.contains("db.ext_tags")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), "");
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))");

    /// The DEFAULT expression of the `id` column of the external table is recorded as the generator.
    inputs.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UInt64", "cityHash64(tags)");
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS db.ext_tags", inputs);
    EXPECT_TRUE(definition.contains("id_generator = 'cityHash64(tags)'")) << definition;

    /// An explicit `id_generator` wins over the DEFAULT expression.
    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_generator = 'sipHash64(tags)' TAGS db.ext_tags", inputs);
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_FALSE(definition.contains("cityHash64")) << definition;

    /// The `id_type` setting must match the type of the `id` column of the external table.
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS id_type = 'UUID' TAGS db.ext_tags", inputs);
    }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The external table must have all the required columns.
    inputs.external_target_columns[ViewTarget::Tags] = makeColumns({makeColumn("id", "UInt64"), makeColumn("metric_name", "String")});
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS db.ext_tags", inputs);
    }), ErrorCodes::THERE_IS_NO_COLUMN);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExternalTargetTablesDefineTypes)
{
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.external_target_columns[ViewTarget::Samples] = makeColumns(
        {makeColumn("id", "UInt64"), makeColumn("timestamp", "DateTime64(6)"), makeColumn("value", "Float32")});
    inputs.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UInt64");
    inputs.external_target_columns[ViewTarget::Metrics] = externalMetricsColumns();

    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES db.ext_samples TAGS db.ext_tags METRICS db.ext_metrics", inputs);
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(6), Float32))")) << definition;
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_FALSE(definition.contains("INNER")) << definition;

    /// The types of the external tables must match each other.
    inputs.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UUID");
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES db.ext_samples TAGS db.ext_tags METRICS db.ext_metrics", inputs);
    }), ErrorCodes::BAD_TYPE_OF_FIELD);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsTableWithExternalTargetTables)
{
    /// The stored definition of a table with external target tables keeps the `id` type in the `id_type` setting,
    /// so the columns of the external tables are not needed to create a table `AS` that table.
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.as_create_query = parseCreateQuery(
        "CREATE TABLE db.src (`time_series` Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries "
        "SETTINGS id_type = 'UInt64', id_generator = 'sipHash64(tags)', version = 2, recent_samples_ttl_seconds = 345600 "
        "SAMPLES db.ext_samples RECENT SAMPLES db.ext_recent_samples TAGS db.ext_tags METRICS db.ext_metrics");

    auto definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries "
        "SAMPLES INNER COLUMNS (extra UInt8) RECENT SAMPLES INNER COLUMNS (extra UInt8) TAGS INNER COLUMNS (extra UInt8) METRICS INNER COLUMNS (extra UInt8)",
        inputs);

    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(DateTime64(6), Float64))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3)), `extra` UInt8");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64, `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), "
        "`min_time` SimpleAggregateFunction(min, Nullable(DateTime64(6))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(6))), `extra` UInt8");
    EXPECT_FALSE(definition.contains("ext_")) << definition;

    /// The external target tables of the other table are not copied, the new table must declare its own targets.
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", inputs);
    }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsTableWithAnotherIdType)
{
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.as_create_query = parseCreateQuery(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS id_type = 'UInt64', id_generator = 'sipHash64(tags)', version = 2, recent_samples_ttl_seconds = 345600 "
        "SAMPLES INNER COLUMNS (`id` UInt64) TAGS INNER COLUMNS (`id` UInt64)");

    /// The settings written for the `id` type of the other table are copied only if the `id` type stays the same.
    auto definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", inputs);
    EXPECT_TRUE(definition.contains("id_type = 'UInt64'")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, ")) << definition;

    definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID)", inputs);
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UUID DEFAULT reinterpretAsUUID(sipHash128(tags)), ")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "SAMPLES").starts_with("`id` UUID, ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, EarlierVersionDoesNotRecordIdType)
{
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UInt64");

    /// A table pinned to version 1 is defined the way version 1 did it: an older server must be able to read it.
    auto definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1 TAGS db.ext_tags", inputs);
    EXPECT_TRUE(definition.contains("version = 1")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_FALSE(definition.contains("id_generator")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))");

    definition = normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, id_generator = 'sipHash64(tags)' TAGS INNER COLUMNS (id UInt64)");
    EXPECT_FALSE(definition.contains("id_type")) << definition;

    /// The setting itself is rejected for an earlier version.
    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, id_type = 'UInt64'");
    }), ErrorCodes::INVALID_SETTING_VALUE);

    /// The `id_type` copied from the other table is dropped if this table is pinned to an earlier version,
    /// while the `id` type is still inherited from it.
    inputs.as_create_query = parseCreateQuery(
        "CREATE TABLE db.src (`time_series` Array(Tuple(DateTime64(3), Float64))) ENGINE = TimeSeries "
        "SETTINGS id_type = 'UInt64', id_generator = 'sipHash64(tags)', version = 2, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags METRICS db.ext_metrics");
    inputs.external_target_columns.clear();
    definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS version = 1 "
        "SAMPLES INNER COLUMNS (extra UInt8) TAGS INNER COLUMNS (extra UInt8) METRICS INNER COLUMNS (extra UInt8)",
        inputs);
    EXPECT_TRUE(definition.contains("version = 1")) << definition;
    EXPECT_FALSE(definition.contains("id_type")) << definition;
    EXPECT_TRUE(definition.contains("id_generator = 'sipHash64(tags)'")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "SAMPLES").starts_with("`id` UInt64, ")) << definition;
    EXPECT_TRUE(extractInnerColumns(definition, "TAGS").starts_with("`id` UInt64, `metric_name` ")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsNonTimeSeriesTable)
{
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.as_create_query = parseCreateQuery("CREATE TABLE db.src (x UInt8) ENGINE = MergeTree ORDER BY x");
    EXPECT_EQ(getExceptionCode([&]
    {
        normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", inputs);
    }), ErrorCodes::INCORRECT_QUERY);
}


TEST_F(NormalizeTimeSeriesDefinitionTest, ExistingTableNeedsNoInputs)
{
    /// On ATTACH the external target tables may not be loaded yet, so they are not read.
    auto definition = normalizeExistingTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1, recent_samples_ttl_seconds = 0 "
        "SAMPLES db.ext_samples TAGS db.ext_tags METRICS db.ext_metrics");
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


/// A logical error aborts the process in debug and sanitizer builds.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST_F(NormalizeTimeSeriesDefinitionTest, MissingInputsForNewTable)
{
    /// The wrapper `normalizeTimeSeriesDefinition` must pass everything a new table needs.
    EXPECT_EQ(getExceptionCode([]
    {
        auto create_query = parseCreateQuery("CREATE TABLE db.ts ENGINE = TimeSeries");
        normalizeTimeSeriesDefinitionImpl(*create_query, LoadingStrictnessLevel::CREATE, /* is_restore_from_backup = */ false, /* inputs = */ {});
    }), ErrorCodes::LOGICAL_ERROR);

    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries TAGS db.ext_tags");
    }), ErrorCodes::LOGICAL_ERROR);

    EXPECT_EQ(getExceptionCode([]
    {
        normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries");
    }), ErrorCodes::LOGICAL_ERROR);
}
#endif
