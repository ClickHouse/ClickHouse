#include <Core/Settings.h>
#include <Common/SipHash.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionsComparison.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool validate_enum_literals_in_operators;
    extern const SettingsBool use_variant_default_implementation_for_comparisons;
    extern const SettingsDateTimeInputFormat cast_string_to_date_time_mode;
}

ComparisonParams::ComparisonParams(const ContextPtr & context)
    : check_decimal_overflow(decimalCheckComparisonOverflow(context))
    , validate_enum_literals_in_operators(context->getSettingsRef()[Setting::validate_enum_literals_in_operators])
    , use_variant_default_implementation(context->getSettingsRef()[Setting::use_variant_default_implementation_for_comparisons])
    , format_settings(getFormatSettings(context))
    , format_settings_hash(getFormatSettingsHash(context->getSettingsRef()))
{
    format_settings.date_time_input_format = context->getSettingsRef()[Setting::cast_string_to_date_time_mode];
}

void ComparisonParams::updateHash(SipHash & hash) const
{
    hash.update(check_decimal_overflow);
    hash.update(validate_enum_literals_in_operators);
    hash.update(use_variant_default_implementation);
    /// Overridden from `cast_string_to_date_time_mode` above, so it is not part of `format_settings_hash`.
    hash.update(format_settings.date_time_input_format);
    hash.update(format_settings_hash);
}

}
