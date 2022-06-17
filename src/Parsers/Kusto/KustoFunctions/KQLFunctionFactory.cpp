
#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

namespace DB
{
    std::unordered_map <String,KQLFunctionValue> KQLFunctionFactory::kql_functions =
    {
        {"datetime", KQLFunctionValue::datetime},
        {"ago", KQLFunctionValue::ago},
        {"datetime_add", KQLFunctionValue::datetime_add},
        {"datetime_part", KQLFunctionValue::datetime_part},
        {"datetime_diff", KQLFunctionValue::datetime_diff},
        {"dayofmonth", KQLFunctionValue::dayofmonth},
        {"dayofweek", KQLFunctionValue::dayofweek},
        {"dayofyear", KQLFunctionValue::dayofyear},
        {"endofday", KQLFunctionValue::endofday},
        {"endofweek", KQLFunctionValue::endofweek},
        {"endofyear", KQLFunctionValue::endofyear},
        {"format_datetime", KQLFunctionValue::format_datetime},
        {"format_timespan", KQLFunctionValue::format_timespan},
        {"getmonth", KQLFunctionValue::getmonth},
        {"getyear", KQLFunctionValue::getyear},
        {"hoursofday", KQLFunctionValue::hoursofday},
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
        {"weekofyear", KQLFunctionValue::weekofyear},

        {"base64_encode_tostring", KQLFunctionValue::base64_encode_tostring},
        {"base64_encode_fromguid", KQLFunctionValue::base64_encode_fromguid},
        {"base64_decode_tostring", KQLFunctionValue::base64_decode_tostring},
        {"base64_decode_toarray", KQLFunctionValue::base64_decode_toarray},
        {"base64_decode_toguid", KQLFunctionValue::base64_decode_toguid},
        {"countof", KQLFunctionValue::countof},
        {"extract", KQLFunctionValue::extract},
        {"extract_all", KQLFunctionValue::extract_all},
        {"extractjson", KQLFunctionValue::extractjson},
        {"has_any_index", KQLFunctionValue::has_any_index},
        {"indexof", KQLFunctionValue::indexof},
        {"isempty", KQLFunctionValue::isempty},
        {"isnotempty", KQLFunctionValue::isnotempty},
        {"isnotnull", KQLFunctionValue::isnotnull},
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
        {"toupper", KQLFunctionValue::toupper},
        {"translate", KQLFunctionValue::translate},
        {"trim", KQLFunctionValue::trim},
        {"trim_end", KQLFunctionValue::trim_end},
        {"trim_start", KQLFunctionValue::trim_start},
        {"url_decode", KQLFunctionValue::url_decode},
        {"url_encode", KQLFunctionValue::url_encode},

        {"array_concat", KQLFunctionValue::array_concat},
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
        {"toreal", KQLFunctionValue::todouble},
        {"tostring", KQLFunctionValue::tostring},
        {"totimespan", KQLFunctionValue::totimespan},

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
        {"bin", KQLFunctionValue::bin}
    };


std::unique_ptr<IParserKQLFunction> KQLFunctionFactory::get(String &kql_function)
{
/*    if (kql_function=="strrep")  
        return std::make_unique<StrRep>();
    else if (kql_function=="strcat")  
        return std::make_unique<StrCat>();
    else
        return nullptr;*/
    if (kql_functions.find(kql_function) == kql_functions.end())
        return nullptr;

    auto kql_function_id = kql_functions[kql_function];
    switch (kql_function_id)
    {
        case KQLFunctionValue::none:
            return nullptr;

        case KQLFunctionValue::timespan:
            return nullptr;

        case KQLFunctionValue::datetime:
            return nullptr;

        case KQLFunctionValue::ago:
            return nullptr;

        case KQLFunctionValue::datetime_add:
            return nullptr;

        case KQLFunctionValue::datetime_part:
            return nullptr;

        case KQLFunctionValue::datetime_diff:
            return nullptr;

        case KQLFunctionValue::dayofmonth:
            return nullptr;

        case KQLFunctionValue::dayofweek:
            return nullptr;

        case KQLFunctionValue::dayofyear:
            return nullptr;

        case KQLFunctionValue::endofday:
            return nullptr;

        case KQLFunctionValue::endofweek:
            return nullptr;

        case KQLFunctionValue::endofyear:
            return nullptr;

        case KQLFunctionValue::format_datetime:
            return nullptr;

        case KQLFunctionValue::format_timespan:
            return nullptr;

        case KQLFunctionValue::getmonth:
            return nullptr;

        case KQLFunctionValue::getyear:
            return nullptr;

        case KQLFunctionValue::hoursofday:
            return nullptr;

        case KQLFunctionValue::make_timespan:
            return nullptr;

        case KQLFunctionValue::make_datetime:
            return nullptr;

        case KQLFunctionValue::now:
            return nullptr;

        case KQLFunctionValue::startofday:
            return nullptr;

        case KQLFunctionValue::startofmonth:
            return nullptr;

        case KQLFunctionValue::startofweek:
            return nullptr;

        case KQLFunctionValue::startofyear:
            return nullptr;

        case KQLFunctionValue::unixtime_microseconds_todatetime:
            return nullptr;

        case KQLFunctionValue::unixtime_milliseconds_todatetime:
            return nullptr;

        case KQLFunctionValue::unixtime_nanoseconds_todatetime:
            return nullptr;

        case KQLFunctionValue::unixtime_seconds_todatetime:
            return nullptr;

        case KQLFunctionValue::weekofyear:
            return nullptr;


        case KQLFunctionValue::base64_encode_tostring:
            return nullptr;

        case KQLFunctionValue::base64_encode_fromguid:
            return nullptr;

        case KQLFunctionValue::base64_decode_tostring:
            return nullptr;

        case KQLFunctionValue::base64_decode_toarray:
            return nullptr;

        case KQLFunctionValue::base64_decode_toguid:
            return nullptr;

        case KQLFunctionValue::countof:
            return nullptr;

        case KQLFunctionValue::extract:
            return nullptr;

        case KQLFunctionValue::extract_all:
            return nullptr;

        case KQLFunctionValue::extractjson:
            return nullptr;

        case KQLFunctionValue::has_any_index:
            return nullptr;

        case KQLFunctionValue::indexof:
            return nullptr;

        case KQLFunctionValue::isempty:
            return nullptr;

        case KQLFunctionValue::isnotempty:
            return nullptr;

        case KQLFunctionValue::isnotnull:
            return nullptr;

        case KQLFunctionValue::isnull:
            return nullptr;

        case KQLFunctionValue::parse_command_line:
            return nullptr;

        case KQLFunctionValue::parse_csv:
            return nullptr;

        case KQLFunctionValue::parse_json:
            return nullptr;

        case KQLFunctionValue::parse_url:
            return nullptr;

        case KQLFunctionValue::parse_urlquery:
            return nullptr;

        case KQLFunctionValue::parse_version:
            return nullptr;

        case KQLFunctionValue::replace_regex:
            return nullptr;

        case KQLFunctionValue::reverse:
            return nullptr;

        case KQLFunctionValue::split:
            return nullptr;

        case KQLFunctionValue::strcat:
            return std::make_unique<StrCat>();

        case KQLFunctionValue::strcat_delim:
            return nullptr;

        case KQLFunctionValue::strcmp:
            return nullptr;

        case KQLFunctionValue::strlen:
            return nullptr;

        case KQLFunctionValue::strrep:
            return std::make_unique<StrRep>();

        case KQLFunctionValue::substring:
            return nullptr;

        case KQLFunctionValue::toupper:
            return nullptr;

        case KQLFunctionValue::translate:
            return nullptr;

        case KQLFunctionValue::trim:
            return nullptr;

        case KQLFunctionValue::trim_end:
            return nullptr;

        case KQLFunctionValue::trim_start:
            return nullptr;

        case KQLFunctionValue::url_decode:
            return nullptr;

        case KQLFunctionValue::url_encode:
            return nullptr;

        case KQLFunctionValue::array_concat:
            return nullptr;

        case KQLFunctionValue::array_iif:
            return nullptr;

        case KQLFunctionValue::array_index_of:
            return nullptr;

        case KQLFunctionValue::array_length:
            return nullptr;

        case KQLFunctionValue::array_reverse:
            return nullptr;

        case KQLFunctionValue::array_rotate_left:
            return nullptr;

        case KQLFunctionValue::array_rotate_right:
            return nullptr;

        case KQLFunctionValue::array_shift_left:
            return nullptr;

        case KQLFunctionValue::array_shift_right:
            return nullptr;

        case KQLFunctionValue::array_slice:
            return nullptr;

        case KQLFunctionValue::array_sort_asc:
            return nullptr;

        case KQLFunctionValue::array_sort_desc:
            return nullptr;

        case KQLFunctionValue::array_split:
            return nullptr;

        case KQLFunctionValue::array_sum:
            return nullptr;

        case KQLFunctionValue::bag_keys:
            return nullptr;

        case KQLFunctionValue::bag_merge:
            return nullptr;

        case KQLFunctionValue::bag_remove_keys:
            return nullptr;

        case KQLFunctionValue::jaccard_index:
            return nullptr;

        case KQLFunctionValue::pack:
            return nullptr;

        case KQLFunctionValue::pack_all:
            return nullptr;

        case KQLFunctionValue::pack_array:
            return nullptr;

        case KQLFunctionValue::repeat:
            return nullptr;

        case KQLFunctionValue::set_difference:
            return nullptr;

        case KQLFunctionValue::set_has_element:
            return nullptr;

        case KQLFunctionValue::set_intersect:
            return nullptr;

        case KQLFunctionValue::set_union:
            return nullptr;

        case KQLFunctionValue::treepath:
            return nullptr;

        case KQLFunctionValue::zip:
            return nullptr;

        case KQLFunctionValue::tobool:
            return std::make_unique<Tobool>();

        case KQLFunctionValue::todatetime:
            return std::make_unique<ToDatetime>();

        case KQLFunctionValue::todouble:
            return std::make_unique<ToDouble>();

        case KQLFunctionValue::toint:
            return std::make_unique<ToInt>();

        case KQLFunctionValue::tostring:
            return std::make_unique<ToString>();

        case KQLFunctionValue::totimespan:
            return std::make_unique<ToTimespan>();

        case KQLFunctionValue::arg_max:
            return nullptr;

        case KQLFunctionValue::arg_min:
            return nullptr;

        case KQLFunctionValue::avg:
            return nullptr;

        case KQLFunctionValue::avgif:
            return nullptr;

        case KQLFunctionValue::binary_all_and:
            return nullptr;

        case KQLFunctionValue::binary_all_or:
            return nullptr;

        case KQLFunctionValue::binary_all_xor:
            return nullptr;
        case KQLFunctionValue::buildschema:
            return nullptr;

        case KQLFunctionValue::count:
            return nullptr;

        case KQLFunctionValue::countif:
            return nullptr;

        case KQLFunctionValue::dcount:
            return nullptr;

        case KQLFunctionValue::dcountif:
            return nullptr;

        case KQLFunctionValue::make_bag:
            return nullptr;

        case KQLFunctionValue::make_bag_if:
            return nullptr;

        case KQLFunctionValue::make_list:
            return nullptr;

        case KQLFunctionValue::make_list_if:
            return nullptr;

        case KQLFunctionValue::make_list_with_nulls:
            return nullptr;

        case KQLFunctionValue::make_set:
            return nullptr;

        case KQLFunctionValue::make_set_if:
            return nullptr;

        case KQLFunctionValue::max:
            return nullptr;

        case KQLFunctionValue::maxif:
            return nullptr;

        case KQLFunctionValue::min:
            return nullptr;

        case KQLFunctionValue::minif:
            return nullptr;

        case KQLFunctionValue::percentiles:
            return nullptr;

        case KQLFunctionValue::percentiles_array:
            return nullptr;

        case KQLFunctionValue::percentilesw:
            return nullptr;

        case KQLFunctionValue::percentilesw_array:
            return nullptr;

        case KQLFunctionValue::stdev:
            return nullptr;

        case KQLFunctionValue::stdevif:
            return nullptr;

        case KQLFunctionValue::sum:
            return nullptr;

        case KQLFunctionValue::sumif:
            return nullptr;

        case KQLFunctionValue::take_any:
            return nullptr;

        case KQLFunctionValue::take_anyif:
            return nullptr;

        case KQLFunctionValue::variance:
            return nullptr;

        case KQLFunctionValue::varianceif:
            return nullptr;


        case KQLFunctionValue::series_fir:
            return nullptr;

        case KQLFunctionValue::series_iir:
            return nullptr;

        case KQLFunctionValue::series_fit_line:
            return nullptr;

        case KQLFunctionValue::series_fit_line_dynamic:
            return nullptr;

        case KQLFunctionValue::series_fit_2lines:
            return nullptr;

        case KQLFunctionValue::series_fit_2lines_dynamic:
            return nullptr;

        case KQLFunctionValue::series_outliers:
            return nullptr;

        case KQLFunctionValue::series_periods_detect:
            return nullptr;

        case KQLFunctionValue::series_periods_validate:
            return nullptr;

        case KQLFunctionValue::series_stats_dynamic:
            return nullptr;

        case KQLFunctionValue::series_stats:
            return nullptr;

        case KQLFunctionValue::series_fill_backward:
            return nullptr;

        case KQLFunctionValue::series_fill_const:
            return nullptr;

        case KQLFunctionValue::series_fill_forward:
            return nullptr;

        case KQLFunctionValue::series_fill_linear:
            return nullptr;


        case KQLFunctionValue::ipv4_compare:
            return nullptr;

        case KQLFunctionValue::ipv4_is_in_range:
            return nullptr;

        case KQLFunctionValue::ipv4_is_match:
            return nullptr;

        case KQLFunctionValue::ipv4_is_private:
            return nullptr;

        case KQLFunctionValue::ipv4_netmask_suffix:
            return nullptr;

        case KQLFunctionValue::parse_ipv4:
            return nullptr;

        case KQLFunctionValue::parse_ipv4_mask:
            return nullptr;

        case KQLFunctionValue::ipv6_compare:
            return nullptr;

        case KQLFunctionValue::ipv6_is_match:
            return nullptr;

        case KQLFunctionValue::parse_ipv6:
            return nullptr;

        case KQLFunctionValue::parse_ipv6_mask:
            return nullptr;

        case KQLFunctionValue::format_ipv4:
            return nullptr;

        case KQLFunctionValue::format_ipv4_mask:
            return nullptr;


        case KQLFunctionValue::binary_and:
            return nullptr;

        case KQLFunctionValue::binary_not:
            return nullptr;

        case KQLFunctionValue::binary_or:
            return nullptr;

        case KQLFunctionValue::binary_shift_left:
            return nullptr;

        case KQLFunctionValue::binary_shift_right:
            return nullptr;

        case KQLFunctionValue::binary_xor:
            return nullptr;

        case KQLFunctionValue::bitset_count_ones:
            return nullptr;

        case KQLFunctionValue::bin:
            return nullptr;
    }
}

}
