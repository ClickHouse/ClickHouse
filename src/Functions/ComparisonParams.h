#pragma once

#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct ComparisonParams
{
    bool check_decimal_overflow = false;
    bool validate_enum_literals_in_operators = false;
    bool use_variant_default_implementation = true;
    FormatSettings format_settings;

    explicit ComparisonParams(const ContextPtr & context);

    ComparisonParams() = default;
};

}
