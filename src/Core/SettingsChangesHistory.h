#pragma once

#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string.hpp>
#include <unordered_map>

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
        for (const auto & split_element : split)
        {
            size_t component;
            if (!tryParse(component, split_element))
                throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
            components.push_back(component);
        }
    }

    ClickHouseVersion(const char * version) : ClickHouseVersion(String(version)) {}

    bool operator<(const ClickHouseVersion & other) const
    {
        return components < other.components;
    }

private:
    std::vector<size_t> components;
};

struct SettingChangesHistory
{
    struct Change
    {
        Change(const Field & value_, const ClickHouseVersion & version_) : value(value_), version(version_) {}

        Field value;
        ClickHouseVersion version;
    };

    SettingChangesHistory(const Field & initial_value_, const std::vector<Change> & changes_) : initial_value(initial_value_), changes(changes_) {}

    Field getValueForVersion(const ClickHouseVersion & version) const
    {
        Field value = initial_value;
        for (const auto & change : changes)
        {
            if (version < change.version)
                return value;
            value = change.value;
        }
        return value;
    }

    Field initial_value;
    std::vector<Change> changes;
};

/// History of settings changes that controls some backward incompatible changes
/// across all ClickHouse versions. It maps setting name to special struct
/// SettingChangesHistory {initial_value, {{changed_value_1, version1}, {changed_value_2, version_2}, ...}}
/// It's used to implement `compatibility` setting (see https://github.com/ClickHouse/ClickHouse/issues/35972)
const std::unordered_map<String, SettingChangesHistory> settings_changes_history =
{
        {"enable_positional_arguments", {false, {{true, "22.7"}}}},
        {"output_format_json_named_tuples_as_objects", {false, {{true, "22.6"}}}},
        {"memory_overcommit_ratio_denominator", {0, {{1073741824, "22.5"}}}},
        {"memory_overcommit_ratio_denominator_for_user", {0, {{1073741824, "22.5"}}}},
        {"allow_settings_after_format_in_insert", {true, {{false, "22.4"}}}},
        {"cast_ipv4_ipv6_default_on_conversion_error", {true, {{false, "22.3"}}}},
        {"input_format_ipv4_default_on_conversion_error", {true, {{false, "22.3"}}}},
        {"input_format_ipv6_default_on_conversion_error", {true, {{false, "22.3"}}}},
        {"stream_like_engine_allow_direct_select", {true, {{false, "21.12"}}}},
        {"output_format_decimal_trailing_zeros", {true, {{false, "21.9"}}}},
        {"use_hedged_requests", {false, {{true, "21.9"}}}},
        {"legacy_column_name_of_tuple_literal", {true, {{false, "21.7"}}}},
        {"async_socket_for_remote", {true, {{false, "21.3"}, {true, "21.5"}}}},
        {"optimize_normalize_count_variants", {false, {{true, "21.3"}}}},
        {"normalize_function_names", {false, {{true, "21.3"}}}},
        {"enable_global_with_statement", {false, {{true, "21.2"}}}},
        {"insert_quorum_parallel", {false, {{true, "21.1"}}}},
        {"input_format_null_as_default", {false, {{true, "21.1"}}}},
        {"optimize_on_insert", {false, {{true, "21.1"}}}},
        {"use_compact_format_in_distributed_parts_names", {false, {{true, "21.1"}}}},
        {"format_regexp_escaping_rule", {"Escaped", {{"Raw", "20.10"}}}},
        {"show_table_uuid_in_table_create_query_if_not_nil", {true, {{false, "20.7"}}}},
        {"input_format_with_names_use_header", {false, {{true, "20.5"}}}},
        {"allow_suspicious_codecs", {true, {{false, "20.5"}}}},
        {"validate_polygons", {false, {{true, "20.4"}}}},
        {"enable_scalar_subquery_optimization", {false, {{true, "19.18"}}}},
        {"any_join_distinct_right_table_keys", {true, {{false, "19.14"}}}},
        {"input_format_defaults_for_omitted_fields", {false, {{true, "19.12"}}}},
        {"max_partitions_per_insert_block", {0, {{100, "19.5"}}}},
        {"enable_optimize_predicate_expression", {0, {{1, "18.12.17"}}}},
};


}
