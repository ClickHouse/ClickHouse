#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>

namespace ProfileEvents
{
    extern const Event QueryConditionCacheHits;
    extern const Event QueryConditionCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryConditionCacheBytes;
    extern const Metric QueryConditionCacheEntries;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool formatdatetime_f_prints_single_zero;
    extern const SettingsBool formatdatetime_f_prints_scale_number_of_digits;
    extern const SettingsBool formatdatetime_parsedatetime_m_is_month_name;
    extern const SettingsBool formatdatetime_format_without_leading_zeros;
    extern const SettingsBool formatdatetime_e_with_space_padding;
    extern const SettingsBool parsedatetime_parse_without_leading_zeros;
    extern const SettingsBool parsedatetime_e_requires_space_padding;
    extern const SettingsBool function_locate_has_mysql_compatible_argument_order;
    extern const SettingsBool least_greatest_legacy_null_behavior;
    extern const SettingsBool h3togeo_lon_lat_result_order;
    extern const SettingsGeoToH3ArgumentOrder geotoh3_argument_order;
    extern const SettingsBool splitby_max_substrings_includes_remaining_string;
    extern const SettingsBool count_matches_stop_at_empty_match;
    extern const SettingsUInt64 function_visible_width_behavior;
    extern const SettingsBool function_json_value_return_type_allow_complex;
    extern const SettingsBool functions_h3_default_if_invalid;
    extern const SettingsBool cast_ipv4_ipv6_default_on_conversion_error;
}

UInt64 queryConditionCacheSettingsSalt(const Settings & settings)
{
    /// Registration rule: a setting belongs here when a function captures it at build time (in its constructor
    /// or `build`), it changes the value the function returns, and nothing about it reaches
    /// `ActionsDAG::Node::updateHash` (which sees only the function name, the result type name, the children and
    /// the constant values). A setting that changes the result *type* is already covered by the type name in the
    /// hash and does not need an entry.
    SipHash hash;
    /// `formatDateTime` / `parseDateTime`.
    hash.update(settings[Setting::formatdatetime_f_prints_single_zero].value);
    hash.update(settings[Setting::formatdatetime_f_prints_scale_number_of_digits].value);
    hash.update(settings[Setting::formatdatetime_parsedatetime_m_is_month_name].value);
    hash.update(settings[Setting::formatdatetime_format_without_leading_zeros].value);
    hash.update(settings[Setting::formatdatetime_e_with_space_padding].value);
    hash.update(settings[Setting::parsedatetime_parse_without_leading_zeros].value);
    hash.update(settings[Setting::parsedatetime_e_requires_space_padding].value);
    /// `locate` swaps its haystack and needle arguments.
    hash.update(settings[Setting::function_locate_has_mysql_compatible_argument_order].value);
    /// `least` / `greatest` propagate or skip NULL arguments.
    hash.update(settings[Setting::least_greatest_legacy_null_behavior].value);
    /// `h3ToGeo` swaps the tuple elements, `geoToH3` swaps the arguments.
    hash.update(settings[Setting::h3togeo_lon_lat_result_order].value);
    hash.update(static_cast<UInt64>(settings[Setting::geotoh3_argument_order].value));
    /// `splitBy*` with `max_substrings`, `countMatches`, `visibleWidth`, `JSON_VALUE`.
    hash.update(settings[Setting::splitby_max_substrings_includes_remaining_string].value);
    hash.update(settings[Setting::count_matches_stop_at_empty_match].value);
    hash.update(settings[Setting::function_visible_width_behavior].value);
    hash.update(settings[Setting::function_json_value_return_type_allow_complex].value);
    /// Return a default instead of throwing: a verdict written by the lenient session must not be served to a
    /// session that is supposed to see the exception.
    hash.update(settings[Setting::functions_h3_default_if_invalid].value);
    hash.update(settings[Setting::cast_ipv4_ipv6_default_on_conversion_error].value);
    return hash.get64();
}

UInt64 queryConditionCacheHash(UInt64 condition_dag_hash, UInt64 settings_salt)
{
    SipHash hash;
    hash.update(condition_dag_hash);
    hash.update(settings_salt);
    return hash.get64();
}

QueryConditionCache::Key QueryConditionCache::makeKey(const UUID & table_id, const String & part_name, UInt64 condition_hash)
{
    SipHash hash;
    hash.update(table_id);
    hash.update(part_name);
    hash.update(condition_hash);
    return hash.get128();
}

String QueryConditionCache::makeFilePartName(const String & path, std::string_view version_token)
{
    /// NUL cannot occur in a file path or in a version token, so it is an unambiguous separator.
    String part_name = path;
    part_name.push_back('\0');
    part_name.append(version_token);
    return part_name;
}

size_t QueryConditionCache::EntryWeight::operator()(const Entry & entry) const
{
    size_t memory = sizeof(Key) + sizeof(Entry);
    /// Estimate the memory size of `std::vector<bool>` (it uses bit-packing internally)
    /// Round up to bytes.
    memory += (entry.matching_marks.capacity() + 7) / 8;
#if defined(DEBUG_OR_SANITIZER_BUILD)
    memory += entry.part_name.capacity() + entry.condition.capacity();
#endif
    return memory;
}

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, CurrentMetrics::QueryConditionCacheBytes, CurrentMetrics::QueryConditionCacheEntries, max_size_in_bytes, 0, size_ratio)
{
}

void QueryConditionCache::write(
    const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
    const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    if (table_id == UUIDHelpers::Nil)
        return; /// Issue #92863: Certain database engines provide no table UUIDs

    Key key = makeKey(table_id, part_name, condition_hash);

#if defined(DEBUG_OR_SANITIZER_BUILD)
    auto load_func = [&](){ return std::make_shared<Entry>(marks_count, table_id, part_name, condition_hash, condition); };
#else
    auto load_func = [&](){ return std::make_shared<Entry>(marks_count); };
#endif

    auto [entry, inserted] = cache.getOrSet(key, load_func);

    /// Try to avoid acquiring the RW lock below (*) by early-ing out. Matters for systems with lots of cores.
    {
        std::shared_lock shared_lock(entry->mutex); /// cheap

        bool need_not_update_marks = true;
        for (const auto & mark_range : mark_ranges)
        {
            /// If the bits are already in the desired state (false), we don't need to update them.
            need_not_update_marks = std::all_of(entry->matching_marks.begin() + mark_range.begin,
                                                entry->matching_marks.begin() + mark_range.end,
                                                [](auto b) { return b == false; });
            if (!need_not_update_marks)
                break;
        }

        /// Do we either have no final mark or final mark is already in the desired state?
        bool need_not_update_final_mark = !has_final_mark || entry->matching_marks[marks_count - 1] == false;

        if (need_not_update_marks && need_not_update_final_mark)
            return;
    }

    {
        std::lock_guard lock(entry->mutex); /// (*)

        chassert(marks_count == entry->matching_marks.size());

        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(entry->matching_marks.begin() + mark_range.begin, entry->matching_marks.begin() + mark_range.end, false);

        if (has_final_mark)
            entry->matching_marks[marks_count - 1] = false;
    }

    LOG_TEST(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, condition: {}, marks_count: {}, has_final_mark: {}",
        inserted ? "Inserted" : "Updated",
        table_id,
        part_name,
        condition_hash,
        condition,
        marks_count,
        has_final_mark);
}

std::optional<QueryConditionCache::MatchingMarks> QueryConditionCache::read(const UUID & table_id, const String & part_name, UInt64 condition_hash, bool increment_profile_events)
{
    if (table_id == UUIDHelpers::Nil)
        return {}; /// Issue #92864: Certain database engines provide no table UUIDs

    Key key = makeKey(table_id, part_name, condition_hash);

    if (auto entry = cache.get(key))
    {
        if (increment_profile_events)
            ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        std::shared_lock lock(entry->mutex);

        LOG_TEST(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {entry->matching_marks};
    }
    else
    {
        if (increment_profile_events)
            ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

        LOG_TEST(
            logger,
            "Could not find entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {};
    }

}

std::vector<QueryConditionCache::Cache::KeyMapped> QueryConditionCache::dump() const
{
    return cache.dump();
}

void QueryConditionCache::clear()
{
    cache.clear();
}

void QueryConditionCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
}

size_t QueryConditionCache::maxSizeInBytes() const
{
    return cache.maxSizeInBytes();
}

QueryConditionCache::Entry::Entry(size_t mark_count)
    : matching_marks(mark_count, true) /// by default, all marks potentially are potential matches, i.e. we can't skip them
{
}


#if defined(DEBUG_OR_SANITIZER_BUILD)
QueryConditionCache::Entry::Entry(
    size_t mark_count_,
    const UUID & table_id_,
    const String & part_name_,
    UInt64 condition_hash_,
    const String & condition_)
    : table_id(table_id_)
    , part_name(part_name_)
    , condition_hash(condition_hash_)
    , condition(condition_)
    , matching_marks(mark_count_, true)
        {}
#endif

}
