#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLMathematicalFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>

namespace DB
{
std::unordered_map<String, KQLFunctionValue> KQLFunctionFactory::kql_functions
    = {{"ago", KQLFunctionValue::ago},
       {"datetime_add", KQLFunctionValue::datetime_add},
       {"datetime_part", KQLFunctionValue::datetime_part},
       {"datetime_diff", KQLFunctionValue::datetime_diff},
       {"dayofmonth", KQLFunctionValue::dayofmonth},
       {"dayofweek", KQLFunctionValue::dayofweek},
       {"dayofyear", KQLFunctionValue::dayofyear},
       {"endofday", KQLFunctionValue::endofday},
       {"endofweek", KQLFunctionValue::endofweek},
       {"endofyear", KQLFunctionValue::endofyear},
       {"endofmonth", KQLFunctionValue::endofmonth},

       {"format_datetime", KQLFunctionValue::format_datetime},
       {"format_timespan", KQLFunctionValue::format_timespan},
       {"getmonth", KQLFunctionValue::getmonth},
       {"getyear", KQLFunctionValue::getyear},
       {"hourofday", KQLFunctionValue::hourofday},
       {"make_timespan", KQLFunctionValue::make_timespan},
       {"make_datetime", KQLFunctionValue::make_datetime},
       {"now", KQLFunctionValue::now},
       {"startofday", KQLFunctionValue::startofday},
       {"startofmonth", KQLFunctionValue::startofmonth},
       {"startofweek", KQLFunctionValue::startofweek},
       {"startofyear", KQLFunctionValue::startofyear},
       {"todatetime", KQLFunctionValue::todatetime},
       {"totimespan", KQLFunctionValue::totimespan},
       {"unixtime_microseconds_todatetime", KQLFunctionValue::unixtime_microseconds_todatetime},
       {"unixtime_milliseconds_todatetime", KQLFunctionValue::unixtime_milliseconds_todatetime},
       {"unixtime_nanoseconds_todatetime", KQLFunctionValue::unixtime_nanoseconds_todatetime},
       {"unixtime_seconds_todatetime", KQLFunctionValue::unixtime_seconds_todatetime},
       {"week_of_year", KQLFunctionValue::week_of_year},
       {"monthofyear", KQLFunctionValue::monthofyear},
       {"base64_encode_tostring", KQLFunctionValue::base64_encode_tostring},
       {"base64_encode_fromguid", KQLFunctionValue::base64_encode_fromguid},
       {"base64_decode_tostring", KQLFunctionValue::base64_decode_tostring},
       {"base64_decode_toarray", KQLFunctionValue::base64_decode_toarray},
       {"base64_decode_toguid", KQLFunctionValue::base64_decode_toguid},
       {"countof", KQLFunctionValue::countof},
       {"extract", KQLFunctionValue::extract},
       {"extract_all", KQLFunctionValue::extract_all},
       {"extract_json", KQLFunctionValue::extract_json},
       {"extractjson", KQLFunctionValue::extract_json},
       {"has_any_index", KQLFunctionValue::has_any_index},
       {"indexof", KQLFunctionValue::indexof},
       {"isempty", KQLFunctionValue::isempty},
       {"isnan", KQLFunctionValue::isnan},
       {"isnotempty", KQLFunctionValue::isnotempty},
       {"notempty", KQLFunctionValue::isnotempty},
       {"isnotnull", KQLFunctionValue::isnotnull},
       {"notnull", KQLFunctionValue::isnotnull},
       {"isnull", KQLFunctionValue::isnull},
       {"parse_command_line", KQLFunctionValue::parse_command_line},
       {"parse_csv", KQLFunctionValue::parse_csv},
       {"parse_json", KQLFunctionValue::parse_json},
       {"parse_url", KQLFunctionValue::parse_url},
       {"parse_urlquery", KQLFunctionValue::parse_urlquery},
       {"parse_version", KQLFunctionValue::parse_version},
       {"replace_regex", KQLFunctionValue::replace_regex},
       {"reverse", KQLFunctionValue::reverse},
       {"split", KQLFunctionValue::split},
       {"strcat", KQLFunctionValue::strcat},
       {"strcat_delim", KQLFunctionValue::strcat_delim},
       {"strcmp", KQLFunctionValue::strcmp},
       {"strlen", KQLFunctionValue::strlen},
       {"strrep", KQLFunctionValue::strrep},
       {"substring", KQLFunctionValue::substring},
       {"tolower", KQLFunctionValue::tolower},
       {"toupper", KQLFunctionValue::toupper},
       {"translate", KQLFunctionValue::translate},
       {"trim", KQLFunctionValue::trim},
       {"trim_end", KQLFunctionValue::trim_end},
       {"trim_start", KQLFunctionValue::trim_start},
       {"url_decode", KQLFunctionValue::url_decode},
       {"url_encode", KQLFunctionValue::url_encode},

       {"array_concat", KQLFunctionValue::array_concat},
       {"array_iff", KQLFunctionValue::array_iif},
       {"array_iif", KQLFunctionValue::array_iif},
       {"array_index_of", KQLFunctionValue::array_index_of},
       {"array_length", KQLFunctionValue::array_length},
       {"array_reverse", KQLFunctionValue::array_reverse},
       {"array_rotate_left", KQLFunctionValue::array_rotate_left},
       {"array_rotate_right", KQLFunctionValue::array_rotate_right},
       {"array_shift_left", KQLFunctionValue::array_shift_left},
       {"array_shift_right", KQLFunctionValue::array_shift_right},
       {"array_slice", KQLFunctionValue::array_slice},
       {"array_sort_asc", KQLFunctionValue::array_sort_asc},
       {"array_sort_desc", KQLFunctionValue::array_sort_desc},
       {"array_split", KQLFunctionValue::array_split},
       {"array_sum", KQLFunctionValue::array_sum},
       {"bag_keys", KQLFunctionValue::bag_keys},
       {"bag_merge", KQLFunctionValue::bag_merge},
       {"bag_remove_keys", KQLFunctionValue::bag_remove_keys},
       {"jaccard_index", KQLFunctionValue::jaccard_index},
       {"pack", KQLFunctionValue::pack},
       {"pack_all", KQLFunctionValue::pack_all},
       {"pack_array", KQLFunctionValue::pack_array},
       {"repeat", KQLFunctionValue::repeat},
       {"set_difference", KQLFunctionValue::set_difference},
       {"set_has_element", KQLFunctionValue::set_has_element},
       {"set_intersect", KQLFunctionValue::set_intersect},
       {"set_union", KQLFunctionValue::set_union},
       {"treepath", KQLFunctionValue::treepath},
       {"zip", KQLFunctionValue::zip},

       {"tobool", KQLFunctionValue::tobool},
       {"toboolean", KQLFunctionValue::tobool},
       {"todouble", KQLFunctionValue::todouble},
       {"toint", KQLFunctionValue::toint},
       {"tolong", KQLFunctionValue::tolong},
       {"toreal", KQLFunctionValue::todouble},
       {"tostring", KQLFunctionValue::tostring},
       {"totimespan", KQLFunctionValue::totimespan},
       {"todecimal", KQLFunctionValue::todecimal},

       {"arg_max", KQLFunctionValue::arg_max},
       {"arg_min", KQLFunctionValue::arg_min},
       {"avg", KQLFunctionValue::avg},
       {"avgif", KQLFunctionValue::avgif},
       {"binary_all_and", KQLFunctionValue::binary_all_and},
       {"binary_all_or", KQLFunctionValue::binary_all_or},
       {"binary_all_xor", KQLFunctionValue::binary_all_xor},
       {"buildschema", KQLFunctionValue::buildschema},
       {"count", KQLFunctionValue::count},
       {"countif", KQLFunctionValue::countif},
       {"dcount", KQLFunctionValue::dcount},
       {"dcountif", KQLFunctionValue::dcountif},
       {"make_bag", KQLFunctionValue::make_bag},
       {"make_bag_if", KQLFunctionValue::make_bag_if},
       {"make_list", KQLFunctionValue::make_list},
       {"make_list_if", KQLFunctionValue::make_list_if},
       {"make_list_with_nulls", KQLFunctionValue::make_list_with_nulls},
       {"make_set", KQLFunctionValue::make_set},
       {"make_set_if", KQLFunctionValue::make_set_if},
       {"max", KQLFunctionValue::max},
       {"maxif", KQLFunctionValue::maxif},
       {"min", KQLFunctionValue::min},
       {"minif", KQLFunctionValue::minif},
       {"percentile", KQLFunctionValue::percentile},
       {"percentilew", KQLFunctionValue::percentilew},
       {"percentiles", KQLFunctionValue::percentiles},
       {"percentiles_array", KQLFunctionValue::percentiles_array},
       {"percentilesw", KQLFunctionValue::percentilesw},
       {"percentilesw_array", KQLFunctionValue::percentilesw_array},
       {"stdev", KQLFunctionValue::stdev},
       {"stdevif", KQLFunctionValue::stdevif},
       {"sum", KQLFunctionValue::sum},
       {"sumif", KQLFunctionValue::sumif},
       {"take_any", KQLFunctionValue::take_any},
       {"take_anyif", KQLFunctionValue::take_anyif},
       {"variance", KQLFunctionValue::variance},
       {"varianceif", KQLFunctionValue::varianceif},

       {"series_fir", KQLFunctionValue::series_fir},
       {"series_iir", KQLFunctionValue::series_iir},
       {"series_fit_line", KQLFunctionValue::series_fit_line},
       {"series_fit_line_dynamic", KQLFunctionValue::series_fit_line_dynamic},
       {"series_fit_2lines", KQLFunctionValue::series_fit_2lines},
       {"series_fit_2lines_dynamic", KQLFunctionValue::series_fit_2lines_dynamic},
       {"series_outliers", KQLFunctionValue::series_outliers},
       {"series_periods_detect", KQLFunctionValue::series_periods_detect},
       {"series_periods_validate", KQLFunctionValue::series_periods_validate},
       {"series_stats_dynamic", KQLFunctionValue::series_stats_dynamic},
       {"series_stats", KQLFunctionValue::series_stats},
       {"series_fill_backward", KQLFunctionValue::series_fill_backward},
       {"series_fill_const", KQLFunctionValue::series_fill_const},
       {"series_fill_forward", KQLFunctionValue::series_fill_forward},
       {"series_fill_linear", KQLFunctionValue::series_fill_linear},

       {"ipv4_compare", KQLFunctionValue::ipv4_compare},
       {"ipv4_is_in_range", KQLFunctionValue::ipv4_is_in_range},
       {"ipv4_is_match", KQLFunctionValue::ipv4_is_match},
       {"ipv4_is_private", KQLFunctionValue::ipv4_is_private},
       {"ipv4_netmask_suffix", KQLFunctionValue::ipv4_netmask_suffix},
       {"parse_ipv4", KQLFunctionValue::parse_ipv4},
       {"parse_ipv4_mask", KQLFunctionValue::parse_ipv4_mask},
       {"ipv6_compare", KQLFunctionValue::ipv6_compare},
       {"ipv6_is_match", KQLFunctionValue::ipv6_is_match},
       {"parse_ipv6", KQLFunctionValue::parse_ipv6},
       {"parse_ipv6_mask", KQLFunctionValue::parse_ipv6_mask},
       {"format_ipv4", KQLFunctionValue::format_ipv4},
       {"format_ipv4_mask", KQLFunctionValue::format_ipv4_mask},

       {"binary_and", KQLFunctionValue::binary_and},
       {"binary_not", KQLFunctionValue::binary_not},
       {"binary_or", KQLFunctionValue::binary_or},
       {"binary_shift_left", KQLFunctionValue::binary_shift_left},
       {"binary_shift_right", KQLFunctionValue::binary_shift_right},
       {"binary_xor", KQLFunctionValue::binary_xor},
       {"bitset_count_ones", KQLFunctionValue::bitset_count_ones},

       {"bin", KQLFunctionValue::bin},
       {"bin_at", KQLFunctionValue::bin_at},
       {"iif", KQLFunctionValue::iif},

       {"bool", KQLFunctionValue::datatype_bool},
       {"boolean", KQLFunctionValue::datatype_bool},
       {"datetime", KQLFunctionValue::datatype_datetime},
       {"date", KQLFunctionValue::datatype_datetime},
       {"dynamic", KQLFunctionValue::datatype_dynamic},
       {"guid", KQLFunctionValue::datatype_guid},
       {"int", KQLFunctionValue::datatype_int},
       {"long", KQLFunctionValue::datatype_long},
       {"real", KQLFunctionValue::datatype_real},
       {"double", KQLFunctionValue::datatype_real},
       {"string", KQLFunctionValue::datatype_string},
       {"timespan", KQLFunctionValue::datatype_timespan},
       {"time", KQLFunctionValue::datatype_timespan},
       {"decimal", KQLFunctionValue::datatype_decimal},
       {"round", KQLFunctionValue::round}
       };


std::unique_ptr<IParserKQLFunction> KQLFunctionFactory::get(String & kql_function, size_t max_query_size)
{
    if (!kql_functions.contains(kql_function))
        return nullptr;

    auto kql_function_id = kql_functions[kql_function];
    switch (kql_function_id)
    {
        case KQLFunctionValue::none:
            return nullptr;

        case KQLFunctionValue::timespan:
            return getParserKQLFunction<TimeSpan>(max_query_size);

        case KQLFunctionValue::ago:
            return getParserKQLFunction<Ago>(max_query_size);

        case KQLFunctionValue::datetime_add:
            return getParserKQLFunction<DatetimeAdd>(max_query_size);

        case KQLFunctionValue::datetime_part:
            return getParserKQLFunction<DatetimePart>(max_query_size);

        case KQLFunctionValue::datetime_diff:
            return getParserKQLFunction<DatetimeDiff>(max_query_size);

        case KQLFunctionValue::dayofmonth:
            return getParserKQLFunction<DayOfMonth>(max_query_size);

        case KQLFunctionValue::dayofweek:
            return getParserKQLFunction<DayOfWeek>(max_query_size);

        case KQLFunctionValue::dayofyear:
            return getParserKQLFunction<DayOfYear>(max_query_size);

        case KQLFunctionValue::endofday:
            return getParserKQLFunction<EndOfDay>(max_query_size);

        case KQLFunctionValue::endofweek:
            return getParserKQLFunction<EndOfWeek>(max_query_size);

        case KQLFunctionValue::endofyear:
            return getParserKQLFunction<EndOfYear>(max_query_size);

        case KQLFunctionValue::endofmonth:
            return getParserKQLFunction<EndOfMonth>(max_query_size);

        case KQLFunctionValue::monthofyear:
            return getParserKQLFunction<MonthOfYear>(max_query_size);

        case KQLFunctionValue::format_datetime:
            return getParserKQLFunction<FormatDateTime>(max_query_size);

        case KQLFunctionValue::format_timespan:
            return getParserKQLFunction<FormatTimeSpan>(max_query_size);

        case KQLFunctionValue::getmonth:
            return getParserKQLFunction<GetMonth>(max_query_size);

        case KQLFunctionValue::getyear:
            return getParserKQLFunction<GetYear>(max_query_size);

        case KQLFunctionValue::hourofday:
            return getParserKQLFunction<HoursOfDay>(max_query_size);

        case KQLFunctionValue::make_timespan:
            return getParserKQLFunction<MakeTimeSpan>(max_query_size);

        case KQLFunctionValue::make_datetime:
            return getParserKQLFunction<MakeDateTime>(max_query_size);

        case KQLFunctionValue::now:
            return getParserKQLFunction<Now>(max_query_size);

        case KQLFunctionValue::startofday:
            return getParserKQLFunction<StartOfDay>(max_query_size);

        case KQLFunctionValue::startofmonth:
            return getParserKQLFunction<StartOfMonth>(max_query_size);

        case KQLFunctionValue::startofweek:
            return getParserKQLFunction<StartOfWeek>(max_query_size);

        case KQLFunctionValue::startofyear:
            return getParserKQLFunction<StartOfYear>(max_query_size);

        case KQLFunctionValue::unixtime_microseconds_todatetime:
            return getParserKQLFunction<UnixTimeMicrosecondsToDateTime>(max_query_size);

        case KQLFunctionValue::unixtime_milliseconds_todatetime:
            return getParserKQLFunction<UnixTimeMillisecondsToDateTime>(max_query_size);

        case KQLFunctionValue::unixtime_nanoseconds_todatetime:
            return getParserKQLFunction<UnixTimeNanosecondsToDateTime>(max_query_size);

        case KQLFunctionValue::unixtime_seconds_todatetime:
            return getParserKQLFunction<UnixTimeSecondsToDateTime>(max_query_size);

        case KQLFunctionValue::week_of_year:
            return getParserKQLFunction<WeekOfYear>(max_query_size);

        case KQLFunctionValue::base64_encode_tostring:
            return getParserKQLFunction<Base64EncodeToString>(max_query_size);

        case KQLFunctionValue::base64_encode_fromguid:
            return getParserKQLFunction<Base64EncodeFromGuid>(max_query_size);

        case KQLFunctionValue::base64_decode_tostring:
            return getParserKQLFunction<Base64DecodeToString>(max_query_size);

        case KQLFunctionValue::base64_decode_toarray:
            return getParserKQLFunction<Base64DecodeToArray>(max_query_size);

        case KQLFunctionValue::base64_decode_toguid:
            return getParserKQLFunction<Base64DecodeToGuid>(max_query_size);

        case KQLFunctionValue::countof:
            return getParserKQLFunction<CountOf>(max_query_size);

        case KQLFunctionValue::extract:
            return getParserKQLFunction<Extract>(max_query_size);

        case KQLFunctionValue::extract_all:
            return getParserKQLFunction<ExtractAll>(max_query_size);

        case KQLFunctionValue::extract_json:
            return getParserKQLFunction<ExtractJSON>(max_query_size);

        case KQLFunctionValue::has_any_index:
            return getParserKQLFunction<HasAnyIndex>(max_query_size);

        case KQLFunctionValue::indexof:
            return getParserKQLFunction<IndexOf>(max_query_size);

        case KQLFunctionValue::isempty:
            return getParserKQLFunction<IsEmpty>(max_query_size);

        case KQLFunctionValue::isnan:
            return getParserKQLFunction<IsNan>(max_query_size);

        case KQLFunctionValue::isnotempty:
            return getParserKQLFunction<IsNotEmpty>(max_query_size);

        case KQLFunctionValue::isnotnull:
            return getParserKQLFunction<IsNotNull>(max_query_size);

        case KQLFunctionValue::isnull:
            return getParserKQLFunction<IsNull>(max_query_size);

        case KQLFunctionValue::parse_command_line:
            return getParserKQLFunction<ParseCommandLine>(max_query_size);

        case KQLFunctionValue::parse_csv:
            return getParserKQLFunction<ParseCSV>(max_query_size);

        case KQLFunctionValue::parse_json:
            return getParserKQLFunction<ParseJSON>(max_query_size);

        case KQLFunctionValue::parse_url:
            return getParserKQLFunction<ParseURL>(max_query_size);

        case KQLFunctionValue::parse_urlquery:
            return getParserKQLFunction<ParseURLQuery>(max_query_size);

        case KQLFunctionValue::parse_version:
            return getParserKQLFunction<ParseVersion>(max_query_size);

        case KQLFunctionValue::replace_regex:
            return getParserKQLFunction<ReplaceRegex>(max_query_size);

        case KQLFunctionValue::reverse:
            return getParserKQLFunction<Reverse>(max_query_size);

        case KQLFunctionValue::split:
            return getParserKQLFunction<Split>(max_query_size);

        case KQLFunctionValue::strcat:
            return getParserKQLFunction<StrCat>(max_query_size);

        case KQLFunctionValue::strcat_delim:
            return getParserKQLFunction<StrCatDelim>(max_query_size);

        case KQLFunctionValue::strcmp:
            return getParserKQLFunction<StrCmp>(max_query_size);

        case KQLFunctionValue::strlen:
            return getParserKQLFunction<StrLen>(max_query_size);

        case KQLFunctionValue::strrep:
            return getParserKQLFunction<StrRep>(max_query_size);

        case KQLFunctionValue::substring:
            return getParserKQLFunction<SubString>(max_query_size);

        case KQLFunctionValue::tolower:
            return getParserKQLFunction<ToLower>(max_query_size);

        case KQLFunctionValue::toupper:
            return getParserKQLFunction<ToUpper>(max_query_size);

        case KQLFunctionValue::translate:
            return getParserKQLFunction<Translate>(max_query_size);

        case KQLFunctionValue::trim:
            return getParserKQLFunction<Trim>(max_query_size);

        case KQLFunctionValue::trim_end:
            return getParserKQLFunction<TrimEnd>(max_query_size);

        case KQLFunctionValue::trim_start:
            return getParserKQLFunction<TrimStart>(max_query_size);

        case KQLFunctionValue::url_decode:
            return getParserKQLFunction<URLDecode>(max_query_size);

        case KQLFunctionValue::url_encode:
            return getParserKQLFunction<URLEncode>(max_query_size);

        case KQLFunctionValue::array_concat:
            return getParserKQLFunction<ArrayConcat>(max_query_size);

        case KQLFunctionValue::array_iif:
            return getParserKQLFunction<ArrayIif>(max_query_size);

        case KQLFunctionValue::array_index_of:
            return getParserKQLFunction<ArrayIndexOf>(max_query_size);

        case KQLFunctionValue::array_length:
            return getParserKQLFunction<ArrayLength>(max_query_size);

        case KQLFunctionValue::array_reverse:
            return getParserKQLFunction<ArrayReverse>(max_query_size);

        case KQLFunctionValue::array_rotate_left:
            return getParserKQLFunction<ArrayRotateLeft>(max_query_size);

        case KQLFunctionValue::array_rotate_right:
            return getParserKQLFunction<ArrayRotateRight>(max_query_size);

        case KQLFunctionValue::array_shift_left:
            return getParserKQLFunction<ArrayShiftLeft>(max_query_size);

        case KQLFunctionValue::array_shift_right:
            return getParserKQLFunction<ArrayShiftRight>(max_query_size);

        case KQLFunctionValue::array_slice:
            return getParserKQLFunction<ArraySlice>(max_query_size);

        case KQLFunctionValue::array_sort_asc:
            return getParserKQLFunction<ArraySortAsc>(max_query_size);

        case KQLFunctionValue::array_sort_desc:
            return getParserKQLFunction<ArraySortDesc>(max_query_size);

        case KQLFunctionValue::array_split:
            return getParserKQLFunction<ArraySplit>(max_query_size);

        case KQLFunctionValue::array_sum:
            return getParserKQLFunction<ArraySum>(max_query_size);

        case KQLFunctionValue::bag_keys:
            return getParserKQLFunction<BagKeys>(max_query_size);

        case KQLFunctionValue::bag_merge:
            return getParserKQLFunction<BagMerge>(max_query_size);

        case KQLFunctionValue::bag_remove_keys:
            return getParserKQLFunction<BagRemoveKeys>(max_query_size);

        case KQLFunctionValue::jaccard_index:
            return getParserKQLFunction<JaccardIndex>(max_query_size);

        case KQLFunctionValue::pack:
            return getParserKQLFunction<Pack>(max_query_size);

        case KQLFunctionValue::pack_all:
            return getParserKQLFunction<PackAll>(max_query_size);

        case KQLFunctionValue::pack_array:
            return getParserKQLFunction<PackArray>(max_query_size);

        case KQLFunctionValue::repeat:
            return getParserKQLFunction<Repeat>(max_query_size);

        case KQLFunctionValue::set_difference:
            return getParserKQLFunction<SetDifference>(max_query_size);

        case KQLFunctionValue::set_has_element:
            return getParserKQLFunction<SetHasElement>(max_query_size);

        case KQLFunctionValue::set_intersect:
            return getParserKQLFunction<SetIntersect>(max_query_size);

        case KQLFunctionValue::set_union:
            return getParserKQLFunction<SetUnion>(max_query_size);

        case KQLFunctionValue::treepath:
            return getParserKQLFunction<TreePath>(max_query_size);

        case KQLFunctionValue::zip:
            return getParserKQLFunction<Zip>(max_query_size);

        case KQLFunctionValue::tobool:
            return getParserKQLFunction<ToBool>(max_query_size);

        case KQLFunctionValue::todatetime:
            return getParserKQLFunction<ToDateTime>(max_query_size);

        case KQLFunctionValue::todouble:
            return getParserKQLFunction<ToDouble>(max_query_size);

        case KQLFunctionValue::toint:
            return getParserKQLFunction<ToInt>(max_query_size);

        case KQLFunctionValue::tolong:
            return getParserKQLFunction<ToLong>(max_query_size);

        case KQLFunctionValue::tostring:
            return getParserKQLFunction<ToString>(max_query_size);

        case KQLFunctionValue::totimespan:
            return getParserKQLFunction<ToTimeSpan>(max_query_size);

        case KQLFunctionValue::todecimal:
            return getParserKQLFunction<ToDecimal>(max_query_size);

        case KQLFunctionValue::arg_max:
            return getParserKQLFunction<ArgMax>(max_query_size);

        case KQLFunctionValue::arg_min:
            return getParserKQLFunction<ArgMin>(max_query_size);

        case KQLFunctionValue::avg:
            return getParserKQLFunction<Avg>(max_query_size);

        case KQLFunctionValue::avgif:
            return getParserKQLFunction<AvgIf>(max_query_size);

        case KQLFunctionValue::binary_all_and:
            return getParserKQLFunction<BinaryAllAnd>(max_query_size);

        case KQLFunctionValue::binary_all_or:
            return getParserKQLFunction<BinaryAllOr>(max_query_size);

        case KQLFunctionValue::binary_all_xor:
            return getParserKQLFunction<BinaryAllXor>(max_query_size);

        case KQLFunctionValue::buildschema:
            return getParserKQLFunction<BuildSchema>(max_query_size);

        case KQLFunctionValue::count:
            return getParserKQLFunction<Count>(max_query_size);

        case KQLFunctionValue::countif:
            return getParserKQLFunction<CountIf>(max_query_size);

        case KQLFunctionValue::dcount:
            return getParserKQLFunction<DCount>(max_query_size);

        case KQLFunctionValue::dcountif:
            return getParserKQLFunction<DCountIf>(max_query_size);

        case KQLFunctionValue::make_bag:
            return getParserKQLFunction<MakeBag>(max_query_size);

        case KQLFunctionValue::make_bag_if:
            return getParserKQLFunction<MakeBagIf>(max_query_size);

        case KQLFunctionValue::make_list:
            return getParserKQLFunction<MakeList>(max_query_size);

        case KQLFunctionValue::make_list_if:
            return getParserKQLFunction<MakeListIf>(max_query_size);

        case KQLFunctionValue::make_list_with_nulls:
            return getParserKQLFunction<MakeListWithNulls>(max_query_size);

        case KQLFunctionValue::make_set:
            return getParserKQLFunction<MakeSet>(max_query_size);

        case KQLFunctionValue::make_set_if:
            return getParserKQLFunction<MakeSetIf>(max_query_size);

        case KQLFunctionValue::max:
            return getParserKQLFunction<Max>(max_query_size);

        case KQLFunctionValue::maxif:
            return getParserKQLFunction<MaxIf>(max_query_size);

        case KQLFunctionValue::min:
            return getParserKQLFunction<Min>(max_query_size);

        case KQLFunctionValue::minif:
            return getParserKQLFunction<MinIf>(max_query_size);

        case KQLFunctionValue::percentile:
            return getParserKQLFunction<Percentile>(max_query_size);

        case KQLFunctionValue::percentilew:
            return getParserKQLFunction<Percentilew>(max_query_size);

        case KQLFunctionValue::percentiles:
            return getParserKQLFunction<Percentiles>(max_query_size);

        case KQLFunctionValue::percentiles_array:
            return getParserKQLFunction<PercentilesArray>(max_query_size);

        case KQLFunctionValue::percentilesw:
            return getParserKQLFunction<Percentilesw>(max_query_size);

        case KQLFunctionValue::percentilesw_array:
            return getParserKQLFunction<PercentileswArray>(max_query_size);

        case KQLFunctionValue::stdev:
            return getParserKQLFunction<Stdev>(max_query_size);

        case KQLFunctionValue::stdevif:
            return getParserKQLFunction<StdevIf>(max_query_size);

        case KQLFunctionValue::sum:
            return getParserKQLFunction<Sum>(max_query_size);

        case KQLFunctionValue::sumif:
            return getParserKQLFunction<SumIf>(max_query_size);

        case KQLFunctionValue::take_any:
            return getParserKQLFunction<TakeAny>(max_query_size);

        case KQLFunctionValue::take_anyif:
            return getParserKQLFunction<TakeAnyIf>(max_query_size);

        case KQLFunctionValue::variance:
            return getParserKQLFunction<Variance>(max_query_size);

        case KQLFunctionValue::varianceif:
            return getParserKQLFunction<VarianceIf>(max_query_size);

        case KQLFunctionValue::series_fir:
            return getParserKQLFunction<SeriesFir>(max_query_size);

        case KQLFunctionValue::series_iir:
            return getParserKQLFunction<SeriesIir>(max_query_size);

        case KQLFunctionValue::series_fit_line:
            return getParserKQLFunction<SeriesFitLine>(max_query_size);

        case KQLFunctionValue::series_fit_line_dynamic:
            return getParserKQLFunction<SeriesFitLineDynamic>(max_query_size);

        case KQLFunctionValue::series_fit_2lines:
            return getParserKQLFunction<SeriesFit2lines>(max_query_size);

        case KQLFunctionValue::series_fit_2lines_dynamic:
            return getParserKQLFunction<SeriesFit2linesDynamic>(max_query_size);

        case KQLFunctionValue::series_outliers:
            return getParserKQLFunction<SeriesOutliers>(max_query_size);

        case KQLFunctionValue::series_periods_detect:
            return getParserKQLFunction<SeriesPeriodsDetect>(max_query_size);

        case KQLFunctionValue::series_periods_validate:
            return getParserKQLFunction<SeriesPeriodsValidate>(max_query_size);

        case KQLFunctionValue::series_stats_dynamic:
            return getParserKQLFunction<SeriesStatsDynamic>(max_query_size);

        case KQLFunctionValue::series_stats:
            return getParserKQLFunction<SeriesStats>(max_query_size);

        case KQLFunctionValue::series_fill_backward:
            return getParserKQLFunction<SeriesFillBackward>(max_query_size);

        case KQLFunctionValue::series_fill_const:
            return getParserKQLFunction<SeriesFillConst>(max_query_size);

        case KQLFunctionValue::series_fill_forward:
            return getParserKQLFunction<SeriesFillForward>(max_query_size);

        case KQLFunctionValue::series_fill_linear:
            return getParserKQLFunction<SeriesFillLinear>(max_query_size);

        case KQLFunctionValue::ipv4_compare:
            return getParserKQLFunction<Ipv4Compare>(max_query_size);

        case KQLFunctionValue::ipv4_is_in_range:
            return getParserKQLFunction<Ipv4IsInRange>(max_query_size);

        case KQLFunctionValue::ipv4_is_match:
            return getParserKQLFunction<Ipv4IsMatch>(max_query_size);

        case KQLFunctionValue::ipv4_is_private:
            return getParserKQLFunction<Ipv4IsPrivate>(max_query_size);

        case KQLFunctionValue::ipv4_netmask_suffix:
            return getParserKQLFunction<Ipv4NetmaskSuffix>(max_query_size);

        case KQLFunctionValue::parse_ipv4:
            return getParserKQLFunction<ParseIpv4>(max_query_size);

        case KQLFunctionValue::parse_ipv4_mask:
            return getParserKQLFunction<ParseIpv4Mask>(max_query_size);

        case KQLFunctionValue::ipv6_compare:
            return getParserKQLFunction<Ipv6Compare>(max_query_size);

        case KQLFunctionValue::ipv6_is_match:
            return getParserKQLFunction<Ipv6IsMatch>(max_query_size);

        case KQLFunctionValue::parse_ipv6:
            return getParserKQLFunction<ParseIpv6>(max_query_size);

        case KQLFunctionValue::parse_ipv6_mask:
            return getParserKQLFunction<ParseIpv6Mask>(max_query_size);

        case KQLFunctionValue::format_ipv4:
            return getParserKQLFunction<FormatIpv4>(max_query_size);

        case KQLFunctionValue::format_ipv4_mask:
            return getParserKQLFunction<FormatIpv4Mask>(max_query_size);

        case KQLFunctionValue::binary_and:
            return getParserKQLFunction<BinaryAnd>(max_query_size);

        case KQLFunctionValue::binary_not:
            return getParserKQLFunction<BinaryNot>(max_query_size);

        case KQLFunctionValue::binary_or:
            return getParserKQLFunction<BinaryOr>(max_query_size);

        case KQLFunctionValue::binary_shift_left:
            return getParserKQLFunction<BinaryShiftLeft>(max_query_size);

        case KQLFunctionValue::binary_shift_right:
            return getParserKQLFunction<BinaryShiftRight>(max_query_size);

        case KQLFunctionValue::binary_xor:
            return getParserKQLFunction<BinaryXor>(max_query_size);

        case KQLFunctionValue::bitset_count_ones:
            return getParserKQLFunction<BitsetCountOnes>(max_query_size);

        case KQLFunctionValue::bin:
            return getParserKQLFunction<Bin>(max_query_size);

        case KQLFunctionValue::bin_at:
            return getParserKQLFunction<BinAt>(max_query_size);

        case KQLFunctionValue::iif:
            return getParserKQLFunction<Iif>(max_query_size);

        case KQLFunctionValue::datatype_bool:
            return getParserKQLFunction<DatatypeBool>(max_query_size);

        case KQLFunctionValue::datatype_datetime:
            return getParserKQLFunction<DatatypeDatetime>(max_query_size);

        case KQLFunctionValue::datatype_dynamic:
            return getParserKQLFunction<DatatypeDynamic>(max_query_size);

        case KQLFunctionValue::datatype_guid:
            return getParserKQLFunction<DatatypeGuid>(max_query_size);

        case KQLFunctionValue::datatype_int:
            return getParserKQLFunction<DatatypeInt>(max_query_size);

        case KQLFunctionValue::datatype_long:
            return getParserKQLFunction<DatatypeLong>(max_query_size);

        case KQLFunctionValue::datatype_real:
            return getParserKQLFunction<DatatypeReal>(max_query_size);

        case KQLFunctionValue::datatype_string:
            return getParserKQLFunction<DatatypeString>(max_query_size);

        case KQLFunctionValue::datatype_timespan:
            return getParserKQLFunction<DatatypeTimespan>(max_query_size);

        case KQLFunctionValue::datatype_decimal:
            return getParserKQLFunction<DatatypeDecimal>(max_query_size);

        case KQLFunctionValue::round:
            return getParserKQLFunction<Round>(max_query_size);
    }
}

}
