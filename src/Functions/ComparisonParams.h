#pragma once

#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>

class SipHash;

namespace DB
{

struct ComparisonParams
{
    bool check_decimal_overflow = false;
    bool validate_enum_literals_in_operators = false;
    bool use_variant_default_implementation = true;
    FormatSettings format_settings;
    /// The hash of the session settings `format_settings` was derived from (see `getFormatSettingsHash`).
    UInt64 format_settings_hash = 0;

    explicit ComparisonParams(const ContextPtr & context);

    ComparisonParams() = default;

    /// Everything here decides what a comparison produces for the same arguments (how a string literal
    /// is read as a `DateTime`, whether an enum literal or a decimal overflow throws), so a hash that
    /// keys an expression has to see it all. See `IFunctionBase::updateHash`.
    void updateHash(SipHash & hash) const;
};

}
