#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
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
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserSetQuery.h>

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


std::unique_ptr<IParserKQLFunction> KQLFunctionFactory::get(String & kql_function)
{
    if (kql_functions.find(kql_function) == kql_functions.end())
        return nullptr;

    auto kql_function_id = kql_functions[kql_function];
    switch (kql_function_id)
    {
        case KQLFunctionValue::none:
            return nullptr;

        case KQLFunctionValue::timespan:
            return std::make_unique<TimeSpan>();

        case KQLFunctionValue::ago:
            return std::make_unique<Ago>();

        case KQLFunctionValue::datetime_add:
            return std::make_unique<DatetimeAdd>();

        case KQLFunctionValue::datetime_part:
            return std::make_unique<DatetimePart>();

        case KQLFunctionValue::datetime_diff:
            return std::make_unique<DatetimeDiff>();

        case KQLFunctionValue::dayofmonth:
            return std::make_unique<DayOfMonth>();

        case KQLFunctionValue::dayofweek:
            return std::make_unique<DayOfWeek>();

        case KQLFunctionValue::dayofyear:
            return std::make_unique<DayOfYear>();

        case KQLFunctionValue::endofday:
            return std::make_unique<EndOfDay>();

        case KQLFunctionValue::endofweek:
            return std::make_unique<EndOfWeek>();

        case KQLFunctionValue::endofyear:
            return std::make_unique<EndOfYear>();

        case KQLFunctionValue::endofmonth:
            return std::make_unique<EndOfMonth>();

        case KQLFunctionValue::monthofyear:
            return std::make_unique<MonthOfYear>();

        case KQLFunctionValue::format_datetime:
            return std::make_unique<FormatDateTime>();

        case KQLFunctionValue::format_timespan:
            return std::make_unique<FormatTimeSpan>();

        case KQLFunctionValue::getmonth:
            return std::make_unique<GetMonth>();

        case KQLFunctionValue::getyear:
            return std::make_unique<GetYear>();

        case KQLFunctionValue::hourofday:
            return std::make_unique<HoursOfDay>();

        case KQLFunctionValue::make_timespan:
            return std::make_unique<MakeTimeSpan>();

        case KQLFunctionValue::make_datetime:
            return std::make_unique<MakeDateTime>();

        case KQLFunctionValue::now:
            return std::make_unique<Now>();

        case KQLFunctionValue::startofday:
            return std::make_unique<StartOfDay>();

        case KQLFunctionValue::startofmonth:
            return std::make_unique<StartOfMonth>();

        case KQLFunctionValue::startofweek:
            return std::make_unique<StartOfWeek>();

        case KQLFunctionValue::startofyear:
            return std::make_unique<StartOfYear>();

        case KQLFunctionValue::unixtime_microseconds_todatetime:
            return std::make_unique<UnixTimeMicrosecondsToDateTime>();

        case KQLFunctionValue::unixtime_milliseconds_todatetime:
            return std::make_unique<UnixTimeMillisecondsToDateTime>();

        case KQLFunctionValue::unixtime_nanoseconds_todatetime:
            return std::make_unique<UnixTimeNanosecondsToDateTime>();

        case KQLFunctionValue::unixtime_seconds_todatetime:
            return std::make_unique<UnixTimeSecondsToDateTime>();

        case KQLFunctionValue::week_of_year:
            return std::make_unique<WeekOfYear>();

        case KQLFunctionValue::base64_encode_tostring:
            return std::make_unique<Base64EncodeToString>();

        case KQLFunctionValue::base64_encode_fromguid:
            return std::make_unique<Base64EncodeFromGuid>();

        case KQLFunctionValue::base64_decode_tostring:
            return std::make_unique<Base64DecodeToString>();

        case KQLFunctionValue::base64_decode_toarray:
            return std::make_unique<Base64DecodeToArray>();

        case KQLFunctionValue::base64_decode_toguid:
            return std::make_unique<Base64DecodeToGuid>();

        case KQLFunctionValue::countof:
            return std::make_unique<CountOf>();

        case KQLFunctionValue::extract:
            return std::make_unique<Extract>();

        case KQLFunctionValue::extract_all:
            return std::make_unique<ExtractAll>();

        case KQLFunctionValue::extract_json:
            return std::make_unique<ExtractJSON>();

        case KQLFunctionValue::has_any_index:
            return std::make_unique<HasAnyIndex>();

        case KQLFunctionValue::indexof:
            return std::make_unique<IndexOf>();

        case KQLFunctionValue::isempty:
            return std::make_unique<IsEmpty>();

        case KQLFunctionValue::isnan:
            return std::make_unique<IsNan>();

        case KQLFunctionValue::isnotempty:
            return std::make_unique<IsNotEmpty>();

        case KQLFunctionValue::isnotnull:
            return std::make_unique<IsNotNull>();

        case KQLFunctionValue::isnull:
            return std::make_unique<IsNull>();

        case KQLFunctionValue::parse_command_line:
            return std::make_unique<ParseCommandLine>();

        case KQLFunctionValue::parse_csv:
            return std::make_unique<ParseCSV>();

        case KQLFunctionValue::parse_json:
            return std::make_unique<ParseJSON>();

        case KQLFunctionValue::parse_url:
            return std::make_unique<ParseURL>();

        case KQLFunctionValue::parse_urlquery:
            return std::make_unique<ParseURLQuery>();

        case KQLFunctionValue::parse_version:
            return std::make_unique<ParseVersion>();

        case KQLFunctionValue::replace_regex:
            return std::make_unique<ReplaceRegex>();

        case KQLFunctionValue::reverse:
            return std::make_unique<Reverse>();

        case KQLFunctionValue::split:
            return std::make_unique<Split>();

        case KQLFunctionValue::strcat:
            return std::make_unique<StrCat>();

        case KQLFunctionValue::strcat_delim:
            return std::make_unique<StrCatDelim>();

        case KQLFunctionValue::strcmp:
            return std::make_unique<StrCmp>();

        case KQLFunctionValue::strlen:
            return std::make_unique<StrLen>();

        case KQLFunctionValue::strrep:
            return std::make_unique<StrRep>();

        case KQLFunctionValue::substring:
            return std::make_unique<SubString>();

        case KQLFunctionValue::tolower:
            return std::make_unique<ToLower>();

        case KQLFunctionValue::toupper:
            return std::make_unique<ToUpper>();

        case KQLFunctionValue::translate:
            return std::make_unique<Translate>();

        case KQLFunctionValue::trim:
            return std::make_unique<Trim>();

        case KQLFunctionValue::trim_end:
            return std::make_unique<TrimEnd>();

        case KQLFunctionValue::trim_start:
            return std::make_unique<TrimStart>();

        case KQLFunctionValue::url_decode:
            return std::make_unique<URLDecode>();

        case KQLFunctionValue::url_encode:
            return std::make_unique<URLEncode>();

        case KQLFunctionValue::array_concat:
            return std::make_unique<ArrayConcat>();

        case KQLFunctionValue::array_iif:
            return std::make_unique<ArrayIif>();

        case KQLFunctionValue::array_index_of:
            return std::make_unique<ArrayIndexOf>();

        case KQLFunctionValue::array_length:
            return std::make_unique<ArrayLength>();

        case KQLFunctionValue::array_reverse:
            return std::make_unique<ArrayReverse>();

        case KQLFunctionValue::array_rotate_left:
            return std::make_unique<ArrayRotateLeft>();

        case KQLFunctionValue::array_rotate_right:
            return std::make_unique<ArrayRotateRight>();

        case KQLFunctionValue::array_shift_left:
            return std::make_unique<ArrayShiftLeft>();

        case KQLFunctionValue::array_shift_right:
            return std::make_unique<ArrayShiftRight>();

        case KQLFunctionValue::array_slice:
            return std::make_unique<ArraySlice>();

        case KQLFunctionValue::array_sort_asc:
            return std::make_unique<ArraySortAsc>();

        case KQLFunctionValue::array_sort_desc:
            return std::make_unique<ArraySortDesc>();

        case KQLFunctionValue::array_split:
            return std::make_unique<ArraySplit>();

        case KQLFunctionValue::array_sum:
            return std::make_unique<ArraySum>();

        case KQLFunctionValue::bag_keys:
            return std::make_unique<BagKeys>();

        case KQLFunctionValue::bag_merge:
            return std::make_unique<BagMerge>();

        case KQLFunctionValue::bag_remove_keys:
            return std::make_unique<BagRemoveKeys>();

        case KQLFunctionValue::jaccard_index:
            return std::make_unique<JaccardIndex>();

        case KQLFunctionValue::pack:
            return std::make_unique<Pack>();

        case KQLFunctionValue::pack_all:
            return std::make_unique<PackAll>();

        case KQLFunctionValue::pack_array:
            return std::make_unique<PackArray>();

        case KQLFunctionValue::repeat:
            return std::make_unique<Repeat>();

        case KQLFunctionValue::set_difference:
            return std::make_unique<SetDifference>();

        case KQLFunctionValue::set_has_element:
            return std::make_unique<SetHasElement>();

        case KQLFunctionValue::set_intersect:
            return std::make_unique<SetIntersect>();

        case KQLFunctionValue::set_union:
            return std::make_unique<SetUnion>();

        case KQLFunctionValue::treepath:
            return std::make_unique<TreePath>();

        case KQLFunctionValue::zip:
            return std::make_unique<Zip>();

        case KQLFunctionValue::tobool:
            return std::make_unique<ToBool>();

        case KQLFunctionValue::todatetime:
            return std::make_unique<ToDateTime>();

        case KQLFunctionValue::todouble:
            return std::make_unique<ToDouble>();

        case KQLFunctionValue::toint:
            return std::make_unique<ToInt>();

        case KQLFunctionValue::tolong:
            return std::make_unique<ToLong>();

        case KQLFunctionValue::tostring:
            return std::make_unique<ToString>();

        case KQLFunctionValue::totimespan:
            return std::make_unique<ToTimeSpan>();

        case KQLFunctionValue::todecimal:
            return std::make_unique<ToDecimal>();

        case KQLFunctionValue::arg_max:
            return std::make_unique<ArgMax>();

        case KQLFunctionValue::arg_min:
            return std::make_unique<ArgMin>();

        case KQLFunctionValue::avg:
            return std::make_unique<Avg>();

        case KQLFunctionValue::avgif:
            return std::make_unique<AvgIf>();

        case KQLFunctionValue::binary_all_and:
            return std::make_unique<BinaryAllAnd>();

        case KQLFunctionValue::binary_all_or:
            return std::make_unique<BinaryAllOr>();

        case KQLFunctionValue::binary_all_xor:
            return std::make_unique<BinaryAllXor>();

        case KQLFunctionValue::buildschema:
            return std::make_unique<BuildSchema>();

        case KQLFunctionValue::count:
            return std::make_unique<Count>();

        case KQLFunctionValue::countif:
            return std::make_unique<CountIf>();

        case KQLFunctionValue::dcount:
            return std::make_unique<DCount>();

        case KQLFunctionValue::dcountif:
            return std::make_unique<DCountIf>();

        case KQLFunctionValue::make_bag:
            return std::make_unique<MakeBag>();

        case KQLFunctionValue::make_bag_if:
            return std::make_unique<MakeBagIf>();

        case KQLFunctionValue::make_list:
            return std::make_unique<MakeList>();

        case KQLFunctionValue::make_list_if:
            return std::make_unique<MakeListIf>();

        case KQLFunctionValue::make_list_with_nulls:
            return std::make_unique<MakeListWithNulls>();

        case KQLFunctionValue::make_set:
            return std::make_unique<MakeSet>();

        case KQLFunctionValue::make_set_if:
            return std::make_unique<MakeSetIf>();

        case KQLFunctionValue::max:
            return std::make_unique<Max>();

        case KQLFunctionValue::maxif:
            return std::make_unique<MaxIf>();

        case KQLFunctionValue::min:
            return std::make_unique<Min>();

        case KQLFunctionValue::minif:
            return std::make_unique<MinIf>();

        case KQLFunctionValue::percentile:
            return std::make_unique<Percentile>();

        case KQLFunctionValue::percentilew:
            return std::make_unique<Percentilew>();

        case KQLFunctionValue::percentiles:
            return std::make_unique<Percentiles>();

        case KQLFunctionValue::percentiles_array:
            return std::make_unique<PercentilesArray>();

        case KQLFunctionValue::percentilesw:
            return std::make_unique<Percentilesw>();

        case KQLFunctionValue::percentilesw_array:
            return std::make_unique<PercentileswArray>();

        case KQLFunctionValue::stdev:
            return std::make_unique<Stdev>();

        case KQLFunctionValue::stdevif:
            return std::make_unique<StdevIf>();

        case KQLFunctionValue::sum:
            return std::make_unique<Sum>();

        case KQLFunctionValue::sumif:
            return std::make_unique<SumIf>();

        case KQLFunctionValue::take_any:
            return std::make_unique<TakeAny>();

        case KQLFunctionValue::take_anyif:
            return std::make_unique<TakeAnyIf>();

        case KQLFunctionValue::variance:
            return std::make_unique<Variance>();

        case KQLFunctionValue::varianceif:
            return std::make_unique<VarianceIf>();

        case KQLFunctionValue::series_fir:
            return std::make_unique<SeriesFir>();

        case KQLFunctionValue::series_iir:
            return std::make_unique<SeriesIir>();

        case KQLFunctionValue::series_fit_line:
            return std::make_unique<SeriesFitLine>();

        case KQLFunctionValue::series_fit_line_dynamic:
            return std::make_unique<SeriesFitLineDynamic>();

        case KQLFunctionValue::series_fit_2lines:
            return std::make_unique<SeriesFit2lines>();

        case KQLFunctionValue::series_fit_2lines_dynamic:
            return std::make_unique<SeriesFit2linesDynamic>();

        case KQLFunctionValue::series_outliers:
            return std::make_unique<SeriesOutliers>();

        case KQLFunctionValue::series_periods_detect:
            return std::make_unique<SeriesPeriodsDetect>();

        case KQLFunctionValue::series_periods_validate:
            return std::make_unique<SeriesPeriodsValidate>();

        case KQLFunctionValue::series_stats_dynamic:
            return std::make_unique<SeriesStatsDynamic>();

        case KQLFunctionValue::series_stats:
            return std::make_unique<SeriesStats>();

        case KQLFunctionValue::series_fill_backward:
            return std::make_unique<SeriesFillBackward>();

        case KQLFunctionValue::series_fill_const:
            return std::make_unique<SeriesFillConst>();

        case KQLFunctionValue::series_fill_forward:
            return std::make_unique<SeriesFillForward>();

        case KQLFunctionValue::series_fill_linear:
            return std::make_unique<SeriesFillLinear>();

        case KQLFunctionValue::ipv4_compare:
            return std::make_unique<Ipv4Compare>();

        case KQLFunctionValue::ipv4_is_in_range:
            return std::make_unique<Ipv4IsInRange>();

        case KQLFunctionValue::ipv4_is_match:
            return std::make_unique<Ipv4IsMatch>();

        case KQLFunctionValue::ipv4_is_private:
            return std::make_unique<Ipv4IsPrivate>();

        case KQLFunctionValue::ipv4_netmask_suffix:
            return std::make_unique<Ipv4NetmaskSuffix>();

        case KQLFunctionValue::parse_ipv4:
            return std::make_unique<ParseIpv4>();

        case KQLFunctionValue::parse_ipv4_mask:
            return std::make_unique<ParseIpv4Mask>();

        case KQLFunctionValue::ipv6_compare:
            return std::make_unique<Ipv6Compare>();

        case KQLFunctionValue::ipv6_is_match:
            return std::make_unique<Ipv6IsMatch>();

        case KQLFunctionValue::parse_ipv6:
            return std::make_unique<ParseIpv6>();

        case KQLFunctionValue::parse_ipv6_mask:
            return std::make_unique<ParseIpv6Mask>();

        case KQLFunctionValue::format_ipv4:
            return std::make_unique<FormatIpv4>();

        case KQLFunctionValue::format_ipv4_mask:
            return std::make_unique<FormatIpv4Mask>();

        case KQLFunctionValue::binary_and:
            return std::make_unique<BinaryAnd>();

        case KQLFunctionValue::binary_not:
            return std::make_unique<BinaryNot>();

        case KQLFunctionValue::binary_or:
            return std::make_unique<BinaryOr>();

        case KQLFunctionValue::binary_shift_left:
            return std::make_unique<BinaryShiftLeft>();

        case KQLFunctionValue::binary_shift_right:
            return std::make_unique<BinaryShiftRight>();

        case KQLFunctionValue::binary_xor:
            return std::make_unique<BinaryXor>();

        case KQLFunctionValue::bitset_count_ones:
            return std::make_unique<BitsetCountOnes>();

        case KQLFunctionValue::bin:
            return std::make_unique<Bin>();

        case KQLFunctionValue::bin_at:
            return std::make_unique<BinAt>();

        case KQLFunctionValue::datatype_bool:
            return std::make_unique<DatatypeBool>();

        case KQLFunctionValue::datatype_datetime:
            return std::make_unique<DatatypeDatetime>();

        case KQLFunctionValue::datatype_dynamic:
            return std::make_unique<DatatypeDynamic>();

        case KQLFunctionValue::datatype_guid:
            return std::make_unique<DatatypeGuid>();

        case KQLFunctionValue::datatype_int:
            return std::make_unique<DatatypeInt>();

        case KQLFunctionValue::datatype_long:
            return std::make_unique<DatatypeLong>();

        case KQLFunctionValue::datatype_real:
            return std::make_unique<DatatypeReal>();

        case KQLFunctionValue::datatype_string:
            return std::make_unique<DatatypeString>();

        case KQLFunctionValue::datatype_timespan:
            return std::make_unique<DatatypeTimespan>();

        case KQLFunctionValue::datatype_decimal:
            return std::make_unique<DatatypeDecimal>();

        case KQLFunctionValue::round:
            return std::make_unique<Round>();
    }
}

}
