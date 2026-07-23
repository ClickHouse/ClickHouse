#include <Interpreters/resolveNumberLiteral.h>
#include <Interpreters/convertFieldToType.h>

#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>

#include <algorithm>


namespace DB
{

namespace
{

/// Normalize a numeric-literal string into a plain decimal string (no exponent and no insignificant
/// trailing zeroes) together with its scale, so it can be parsed exactly into a wide Decimal.
/// Returns false when the normalized value does not fit into Decimal256 (the caller then resolves
/// through Float64). Example: "1.5e-3" -> "0.0015" (scale 4); "1.230000e0" -> "1.23" (scale 2).
bool normalizeDecimalLiteral(const String & text, String & out_text, UInt32 & out_scale)
{
    constexpr size_t max_precision = DataTypeDecimal<Decimal256>::maxPrecision();

    std::string_view sv = text;
    const bool negative = !sv.empty() && sv[0] == '-';
    if (!sv.empty() && (sv[0] == '+' || sv[0] == '-'))
        sv.remove_prefix(1);

    /// Split off the exponent.
    Int64 exponent = 0;
    std::string_view mantissa = sv;
    if (auto e_pos = sv.find_first_of("eE"); e_pos != std::string_view::npos)
    {
        mantissa = sv.substr(0, e_pos);
        std::string_view exp_sv = sv.substr(e_pos + 1);
        size_t j = 0;
        const bool exp_negative = j < exp_sv.size() && exp_sv[j] == '-';
        if (j < exp_sv.size() && (exp_sv[j] == '+' || exp_sv[j] == '-'))
            ++j;
        for (; j < exp_sv.size(); ++j)
        {
            if (exponent > 1'000'000) /// Absurdly large; the value can't fit Decimal256 anyway.
                return false;
            exponent = exponent * 10 + (exp_sv[j] - '0');
        }
        if (exp_negative)
            exponent = -exponent;
    }

    /// value = digits * 10^(exponent - number_of_fractional_digits)
    size_t dot = mantissa.find('.');
    std::string_view int_part = dot == std::string_view::npos ? mantissa : mantissa.substr(0, dot);
    std::string_view frac_part = dot == std::string_view::npos ? std::string_view{} : mantissa.substr(dot + 1);

    String digits;
    digits.reserve(int_part.size() + frac_part.size());
    digits.append(int_part);
    digits.append(frac_part);
    Int64 dexp = exponent - static_cast<Int64>(frac_part.size());

    /// Drop insignificant trailing zeroes (each one raises the exponent by 1).
    size_t end = digits.size();
    while (end > 0 && digits[end - 1] == '0')
    {
        --end;
        ++dexp;
    }
    /// Drop leading zeroes (they don't change the value).
    size_t begin = 0;
    while (begin < end && digits[begin] == '0')
        ++begin;

    const std::string_view significant(digits.data() + begin, end - begin);
    if (significant.empty()) /// The value is zero in any spelling.
    {
        out_text = "0";
        out_scale = 0;
        return true;
    }

    if (dexp >= 0)
    {
        /// Integer value: significant digits followed by `dexp` zeroes.
        if (significant.size() + static_cast<size_t>(dexp) > max_precision)
            return false;
        out_text = (negative ? "-" : "") + String(significant) + String(static_cast<size_t>(dexp), '0');
        out_scale = 0;
        return true;
    }

    const size_t scale = static_cast<size_t>(-dexp);
    if (scale > max_precision || std::max(significant.size(), scale) > max_precision)
        return false;

    String result = negative ? "-" : "";
    if (significant.size() > scale)
    {
        result.append(significant.substr(0, significant.size() - scale));
        result.push_back('.');
        result.append(significant.substr(significant.size() - scale));
    }
    else
    {
        result.append("0.");
        result.append(scale - significant.size(), '0');
        result.append(significant);
    }
    out_text = std::move(result);
    out_scale = static_cast<UInt32>(scale);
    return true;
}

}

std::pair<Field, DataTypePtr> resolveNumberLiteralForFunction(
    const String & text, const DataTypePtr & reference_type, bool is_comparison)
{
    auto default_type = applyVisitor(FieldToDataType(), Field(NumberLiteral(text)));
    WhichDataType which_default(default_type);
    WhichDataType which_ref(reference_type ? removeNullable(reference_type) : default_type);

    DataTypePtr target_type = default_type;
    /// For a Decimal comparison the literal is parsed straight into a wide Decimal from this text
    /// (exact) instead of through Float64; it holds the normalized decimal spelling.
    String decimal_text = text;
    if (reference_type)
    {
        auto ref_unwrapped = removeNullable(reference_type);

        if (is_comparison && isDecimal(*ref_unwrapped))
        {
            /// Normalize the literal (fold the exponent, drop insignificant trailing zeroes) and parse
            /// it into a wide Decimal with the resulting scale, so different spellings of the same value
            /// (`1.5e-3` and `0.0015`, or a value padded with trailing zeroes) resolve identically and
            /// exactly. Values that don't fit Decimal256 keep the Float64 default.
            String normalized;
            UInt32 scale = 0;
            if (normalizeDecimalLiteral(text, normalized, scale))
            {
                decimal_text = std::move(normalized);
                target_type = std::make_shared<DataTypeDecimal<Decimal256>>(
                    DataTypeDecimal<Decimal256>::maxPrecision(), scale);
            }
        }
        else if (which_default.isInt() && which_ref.isInt()
                 && default_type->getSizeOfValueInMemory() <= ref_unwrapped->getSizeOfValueInMemory())
        {
            target_type = ref_unwrapped;
        }
        else if (which_default.isUInt() && (which_ref.isUInt() || which_ref.isInt())
                 && default_type->getSizeOfValueInMemory() <= ref_unwrapped->getSizeOfValueInMemory())
        {
            target_type = ref_unwrapped;
        }
        else if (which_default.isFloat() && which_ref.isFloat())
        {
            target_type = ref_unwrapped;
        }
    }

    /// For Decimal targets, convert from the normalized string text directly (preserves precision).
    /// For other targets, resolve the NumberLiteral first (see NumberLiteral::toFloat64 for floats).
    Field parsed_field;
    if (isDecimal(*target_type))
        parsed_field = tryConvertFieldToType(Field(decimal_text), *target_type);
    else
        parsed_field = tryConvertFieldToType(Field(NumberLiteral(text)).resolveNumberLiteral(), *target_type);

    /// If conversion to target type failed, fall back to default type.
    if (parsed_field.isNull() && !target_type->isNullable() && target_type != default_type)
    {
        target_type = default_type;
        if (isDecimal(*target_type))
            parsed_field = tryConvertFieldToType(Field(text), *target_type);
        else
            parsed_field = tryConvertFieldToType(Field(NumberLiteral(text)).resolveNumberLiteral(), *target_type);
    }

    if (!parsed_field.isNull() || (target_type && target_type->isNullable()))
        return {parsed_field, target_type};

    return {Field(), nullptr};
}

}
