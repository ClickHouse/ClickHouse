#pragma once

#include <Core/Field.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string.hpp>
#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ClickHouseVersion
{
public:
    ClickHouseVersion(const String & version)
    {
        Strings split;
        boost::split(split, version, [](char c){ return c == '.'; });
        components.reserve(split.size());
        if (split.empty())
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

        for (const auto & split_element : split)
        {
            size_t component;
            if (!tryParse(component, split_element))
                throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
            components.push_back(component);
        }
    }

    ClickHouseVersion(const char * version) : ClickHouseVersion(String(version)) {}

    String toString() const
    {
        String version = std::to_string(components[0]);
        for (size_t i = 1; i < components.size(); ++i)
            version += "." + std::to_string(components[i]);

        return version;
    }

    bool operator<(const ClickHouseVersion & other) const
    {
        return components < other.components;
    }

    bool operator>=(const ClickHouseVersion & other) const
    {
        return components >= other.components;
    }

private:
    std::vector<size_t> components;
};

namespace SettingsChangesHistory
{
    struct SettingChange
    {
        String name;
        Field previous_value;
        Field new_value;
        String reason;
    };

    using SettingsChanges = std::vector<SettingChange>;
}

/// History of settings changes that controls some backward incompatible changes
/// across all ClickHouse versions. It maps ClickHouse version to settings changes that were done
/// in this version. Settings changes is a vector of structs {setting_name, previous_value, new_value}
/// It's used to implement `compatibility` setting (see https://github.com/ClickHouse/ClickHouse/issues/35972)
static std::map<ClickHouseVersion, SettingsChangesHistory::SettingsChanges> settings_changes_history =
{
        {"22.7", {{"cross_to_inner_join_rewrite", 1, 2, "Force rewrite comma join to inner"},
                  {"enable_positional_arguments", false, true, "Enable positional arguments feature by default"}
                  {"format_csv_allow_single_quotes", true, false, "Most tools don't treat single quote in CSV specially, don't do it by default too"}}},
        {"22.6", {{"output_format_json_named_tuples_as_objects", false, true, "Allow to serialize named tuples as JSON objects in JSON formats by default"}}},
        {"22.5", {{"memory_overcommit_ratio_denominator", 0, 1073741824, "Enable memory overcommit feature by default"},
                  {"memory_overcommit_ratio_denominator_for_user", 0, 1073741824, "Enable memory overcommit feature by default"}}},
        {"22.4", {{"allow_settings_after_format_in_insert", true, false, "Do not allow SETTINGS after FORMAT for INSERT queries because ClickHouse interpret SETTINGS as some values, which is misleading"}}},
        {"22.3", {{"cast_ipv4_ipv6_default_on_conversion_error", true, false, "Make functions cast(value, 'IPv4') and cast(value, 'IPv6') behave same as toIPv4 and toIPv6 functions"}}},
        {"21.12", {{"stream_like_engine_allow_direct_select", true, false, "Do not allow direct select for Kafka/RabbitMQ/FileLog by default"}}},
        {"21.9", {{"output_format_decimal_trailing_zeros", true, false, "Do not output trailing zeros in text representation of Decimal types by default for better looking output"},
                  {"use_hedged_requests", false, true, "Enable Hedged Requests feature bu default"}}},
        {"21.7", {{"legacy_column_name_of_tuple_literal", true, false, "Add this setting only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher"}}},
        {"21.5", {{"async_socket_for_remote", false, true, "Fix all problems and turn on asynchronous reads from socket for remote queries by default again"}}},
        {"21.3", {{"async_socket_for_remote", true, false, "Turn off asynchronous reads from socket for remote queries because of some problems"},
                  {"optimize_normalize_count_variants", false, true, "Rewrite aggregate functions that semantically equals to count() as count() by default"},
                  {"normalize_function_names", false, true, "Normalize function names to their canonical names, this was needed for projection query routing"}}},
        {"21.2", {{"enable_global_with_statement", false, true, "Propagate WITH statements to UNION queries and all subqueries by default"}}},
        {"21.1", {{"insert_quorum_parallel", false, true, "Use parallel quorum inserts by default. It is significantly more convenient to use than sequential quorum inserts"},
                  {"input_format_null_as_default", false, true, "Allow to insert NULL as default for input formats by default"},
                  {"optimize_on_insert", false, true, "Enable data optimization on INSERT by default for better user experience"},
                  {"use_compact_format_in_distributed_parts_names", false, true, "Use compact format for async INSERT into Distributed tables by default"}}},
        {"20.10", {{"format_regexp_escaping_rule", "Escaped", "Raw", "Use Raw as default escaping rule for Regexp format to male the behaviour more like to what users expect"}}},
        {"20.7", {{"show_table_uuid_in_table_create_query_if_not_nil", true, false, "Stop showing  UID of the table in its CREATE query for Engine=Atomic"}}},
        {"20.5", {{"input_format_with_names_use_header", false, true, "Enable using header with names for formats with WithNames/WithNamesAndTypes suffixes"},
                  {"allow_suspicious_codecs", true, false, "Don't allow to specify meaningless compression codecs"}}},
        {"20.4", {{"validate_polygons", false, true, "Throw exception if polygon is invalid in function pointInPolygon by default instead of returning possibly wrong results"}}},
        {"19.18", {{"enable_scalar_subquery_optimization", false, true, "Prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once"}}},
        {"19.14", {{"any_join_distinct_right_table_keys", true, false, "Disable ANY RIGHT and ANY FULL JOINs by default to avoid inconsistency"}}},
        {"19.12", {{"input_format_defaults_for_omitted_fields", false, true, "Enable calculation of complex default expressions for omitted fields for some input formats, because it should be the expected behaviour"}}},
        {"19.5", {{"max_partitions_per_insert_block", 0, 100, "Add a limit for the number of partitions in one block"}}},
        {"18.12.17", {{"enable_optimize_predicate_expression", 0, 1, "Optimize predicates to subqueries by default"}}},
};

}
