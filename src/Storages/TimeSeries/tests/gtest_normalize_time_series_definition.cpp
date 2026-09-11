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

    /// Normalizes the definition of a new table and returns it as it would be stored,
    /// e.g. to be used as the table from the clause `AS <other_table>`.
    boost::intrusive_ptr<const ASTCreateQuery> storedDefinition(const String & query, NormalizeTimeSeriesDefinitionInputs inputs = {})
    {
        static const Settings default_query_settings;
        if (!inputs.query_settings)
            inputs.query_settings = &default_query_settings;
        auto create_query = parseCreateQuery(query);
        normalizeTimeSeriesDefinitionImpl(*create_query, LoadingStrictnessLevel::CREATE, /* is_restore_from_backup = */ false, inputs);
        return create_query;
    }

    /// Normalizes the stored definition of an existing table (as on ATTACH) and returns the normalized definition as text.
    String normalizeExistingTable(const String & query)
    {
        auto create_query = parseCreateQuery(query);
        normalizeTimeSeriesDefinitionImpl(*create_query, LoadingStrictnessLevel::ATTACH, /* is_restore_from_backup = */ false, /* inputs = */ {});
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


TEST_F(NormalizeTimeSeriesDefinitionTest, OuterColumns)
{
    /// The prealpha columns `id`, `timestamp`, `value` are not allowed in the outer column list.
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (id String) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (timestamp String) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (value Int32) ENGINE = TimeSeries"); }), ErrorCodes::INCORRECT_QUERY);

    /// The `time_series` column must have type Array(Tuple(timestamp, value)) with a date/time timestamp and a floating-point value.
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (time_series String) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (time_series Array(Tuple(String, Float64))) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts (time_series Array(Tuple(DateTime64(3), String))) ENGINE = TimeSeries"); }), ErrorCodes::BAD_TYPE_OF_FIELD);

    /// The declared element types of `time_series` propagate to the generated samples columns.
    auto definition = normalizeNewTable("CREATE TABLE db.ts (time_series Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries");
    EXPECT_TRUE(definition.contains("`time_series` Array(Tuple(UInt32, Float32))")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` " + default_id_type + ", `timestamp` UInt32 CODEC(DoubleDelta, ZSTD(1)), `value` Float32 CODEC(ZSTD(3))");

    /// The outer columns are an IO interface which stores no data, so the declared ones are replaced with the canonical list.
    definition = normalizeNewTable("CREATE TABLE db.ts (metric_name Int32, tags String) ENGINE = TimeSeries");
    EXPECT_EQ(extractOuterColumns(definition),
        "`metric_name` String, `tags` Map(String, String), `time_series` Array(Tuple(DateTime64(3), Float64)), "
        "`metric_family` String, `type` String, `unit` String, `help` String");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, DeclaredEnginesWithoutKeysGetGeneratedKeys)
{
    auto definition = normalizeNewTable(
        "CREATE TABLE db.ts ENGINE = TimeSeries SAMPLES ENGINE = MergeTree RECENT SAMPLES ENGINE = MergeTree "
        "TAGS ENGINE = AggregatingMergeTree METRICS ENGINE = ReplacingMergeTree");
    EXPECT_EQ(extractInnerEngine(definition, "SAMPLES"), "MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768");
    EXPECT_EQ(extractInnerEngine(definition, "RECENT SAMPLES"),
        "MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) ORDER BY (id, timestamp) "
        "TTL toDateTime(timestamp) + toIntervalSecond(345600) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1");
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1");
    EXPECT_EQ(extractInnerEngine(definition, "METRICS"), "ReplacingMergeTree ORDER BY metric_family_name");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, VersionSetting)
{
    /// An explicit supported version is accepted, an unknown one is rejected.
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 1").contains("version = 1"));
    EXPECT_TRUE(normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 0").contains("version = 0"));
    EXPECT_EQ(getExceptionCode([] { normalizeNewTable("CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS version = 999"); }), ErrorCodes::INVALID_SETTING_VALUE);

    /// The clause `AS <other_table>` doesn't copy the version: a new table gets the latest one.
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.as_create_query = storedDefinition("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS version = 0");
    auto definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", inputs);
    EXPECT_TRUE(definition.contains("version = 2")) << definition;
    EXPECT_FALSE(definition.contains("version = 0")) << definition;
}


TEST_F(NormalizeTimeSeriesDefinitionTest, NormalizationIsIdempotent)
{
    /// A stored definition is normalized again on ATTACH, and it can be replayed as a new table (e.g. on another replica):
    /// both must keep it unchanged, otherwise the generated parts would be applied twice.
    NormalizeTimeSeriesDefinitionInputs inputs_with_external_tags;
    inputs_with_external_tags.external_target_columns[ViewTarget::Tags] = externalTagsColumns("UInt64", "cityHash64(tags)");

    const std::vector<std::pair<String, NormalizeTimeSeriesDefinitionInputs>> definitions =
    {
        {"CREATE TABLE db.ts ENGINE = TimeSeries", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries SETTINGS recent_samples_partition_by = 'toStartOfHour(timestamp)' "
         "SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD(1)), extra UInt8) TAGS INNER COLUMNS (id UInt64) "
         "TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS index_granularity = 1024", {}},
        {"CREATE TABLE db.ts ENGINE = TimeSeries TAGS db.ext_tags", inputs_with_external_tags},
    };

    for (const auto & [query, inputs] : definitions)
    {
        auto stored = storedDefinition(query, inputs);
        auto stored_text = stored->formatWithSecretsOneLine();

        auto attached = boost::static_pointer_cast<ASTCreateQuery>(stored->clone());
        normalizeTimeSeriesDefinitionImpl(*attached, LoadingStrictnessLevel::ATTACH, /* is_restore_from_backup = */ false, /* inputs = */ {});
        EXPECT_EQ(attached->formatWithSecretsOneLine(), stored_text) << query;

        auto replayed_text = normalizeNewTable(stored_text, inputs);
        EXPECT_EQ(replayed_text, stored_text) << query;
    }
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsCopiesInnerDefinitions)
{
    /// The clause `AS <other_table>` copies the inner columns and engines of the other table, except the parts generated
    /// for the other table: they are generated again for the new table from its settings, while the customized parts are copied.
    NormalizeTimeSeriesDefinitionInputs inputs;

    /// A customized `min_time` column is not copied if the new table doesn't store `min_time`/`max_time`.
    inputs.as_create_query = storedDefinition(
        "CREATE TABLE db.src ENGINE = TimeSeries TAGS INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))) CODEC(ZSTD(1)))");
    auto definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` " + default_id_type + " DEFAULT " + default_id_generator + ", `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String)");
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1");

    /// `tags_to_columns` written in the query replaces the copied one: the column of a removed tag is not copied,
    /// the column of an added tag is generated.
    inputs.as_create_query = storedDefinition("CREATE TABLE db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'instance': 'instance'}");
    definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'instance': 'instance', 'region': 'region'}", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` " + default_id_type + " DEFAULT " + default_id_generator + ", `metric_name` LowCardinality(String), `instance` String, `region` String, "
        "`tags` Map(LowCardinality(String), String), `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))), "
        "`max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))");

    /// The source customizes the codec of `timestamp` and adds an extra column in the samples table, sets the `id` type,
    /// and adds settings to the tags engine; everything else is generated.
    inputs.as_create_query = storedDefinition(
        "CREATE TABLE db.src ENGINE = TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD(1)), extra UInt8) "
        "TAGS INNER COLUMNS (id UInt64) TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0");

    /// The customized parts are copied, the generated parts follow the settings of the new table: `min_time`/`max_time`
    /// are not aggregated, so the tags engine is ReplacingMergeTree with them in the sorting key, and keeps its settings.
    definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"),
        "`id` UInt64, `timestamp` DateTime64(6) CODEC(Delta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3)), `extra` UInt8");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), "
        "`min_time` Nullable(DateTime64(6)), `max_time` Nullable(DateTime64(6))");
    EXPECT_EQ(extractInnerEngine(definition, "TAGS"),
        "ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id, min_time, max_time) "
        "SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, allow_nullable_key = 1");

    /// A type declared in the query wins over the type of the other table, the other types are inherited: `value` is
    /// Float32 as declared, `timestamp` is DateTime64(6) as in the source. The declared samples columns replace the copied ones.
    definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SAMPLES INNER COLUMNS (value Float32)", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), `value` Float32");
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), `value` Float32 CODEC(ZSTD(3))");
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        "`id` UInt64 DEFAULT sipHash64(tags), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), "
        "`min_time` SimpleAggregateFunction(min, Nullable(DateTime64(6))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(6)))");

    /// The inner columns of the other table are not copied for a target replaced with an external table, the types come
    /// from the external table: `timestamp` is DateTime64(3) in the source and DateTime64(6) in the external table.
    inputs.as_create_query = storedDefinition("CREATE TABLE db.src ENGINE = TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(ZSTD(5)))");
    inputs.external_target_columns[ViewTarget::Samples] = makeColumns(
        {makeColumn("id", "UInt64"), makeColumn("timestamp", "DateTime64(6)"), makeColumn("value", "Float64")});
    definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SAMPLES db.ext_data", inputs);
    EXPECT_FALSE(definition.contains("ext_data SAMPLES INNER COLUMNS")) << definition;
    EXPECT_EQ(extractInnerColumns(definition, "RECENT SAMPLES"), "`id` UInt64, `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))");
}


TEST_F(NormalizeTimeSeriesDefinitionTest, CreateAsMergesSettings)
{
    /// The clause `AS <other_table>` copies the settings of the other table. A `SETTINGS` clause written in the query
    /// doesn't replace them: the settings are merged by name, so a written setting overrides the copied one, a written
    /// setting the other table doesn't have is added, and the rest of the other table's settings are still copied.
    NormalizeTimeSeriesDefinitionInputs inputs;
    inputs.as_create_query = storedDefinition(
        "CREATE TABLE db.src ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job'}, store_min_time_and_max_time = 0");

    const String tags_without_min_max = "`id` " + default_id_type + " DEFAULT " + default_id_generator
        + ", `metric_name` LowCardinality(String), `job` String, `tags` Map(LowCardinality(String), String)";

    /// Without a `SETTINGS` clause: `job` comes from the copied `tags_to_columns`,
    /// and there is no `min_time`/`max_time` because the copied `store_min_time_and_max_time` is 0.
    auto definition = normalizeNewTable("CREATE TABLE db.copy AS db.src ENGINE = TimeSeries", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_without_min_max);

    /// The written `store_min_time_and_max_time` overrides the copied one so `min_time`/`max_time` appear,
    /// the written `tags_index_granularity` is added, and `tags_to_columns` is still copied.
    definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 1, tags_index_granularity = 4096", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"),
        tags_without_min_max + ", `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))");
    EXPECT_TRUE(extractInnerEngine(definition, "TAGS").contains("index_granularity = 4096")) << definition;

    /// A setting written as `name = DEFAULT` is a mention of that setting too, so the value of the other table is not
    /// copied for it: the setting gets its default value and `min_time`/`max_time` appear, while `tags_to_columns` is still copied.
    definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0, store_min_time_and_max_time = DEFAULT", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_without_min_max + ", `min_time` Nullable(DateTime64(3)), `max_time` Nullable(DateTime64(3))");

    /// A written value wins over a reset of the same setting, so there is no `min_time`/`max_time`.
    definition = normalizeNewTable(
        "CREATE TABLE db.copy AS db.src ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0, store_min_time_and_max_time = DEFAULT", inputs);
    EXPECT_EQ(extractInnerColumns(definition, "TAGS"), tags_without_min_max);
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
