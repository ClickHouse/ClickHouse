#include <Interpreters/resolveNumberLiteral.h>
#include <Interpreters/convertFieldToType.h>

#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>

#include <algorithm>


namespace DB
{

std::pair<Field, DataTypePtr> resolveNumberLiteralForFunction(
    const String & text, const DataTypePtr & reference_type, bool is_comparison)
{
    auto default_type = applyVisitor(FieldToDataType(), Field(NumberLiteral(text)));
    WhichDataType which_default(default_type);
    WhichDataType which_ref(reference_type ? removeNullable(reference_type) : default_type);

    DataTypePtr target_type = default_type;
    if (reference_type)
    {
        auto ref_unwrapped = removeNullable(reference_type);

        if (is_comparison && isDecimal(*ref_unwrapped))
        {
            /// For comparisons, pick a wide Decimal target whose scale matches the literal's own
            /// digits, so the text is parsed straight into Decimal below (exact) instead of through
            /// Float64. Fold the exponent into the scale (scale = fractional_digits - exponent) so
            /// `1.5e-3` and `0.0015` produce the same scale; the String-to-Decimal parse handles the
            /// exponent, so both resolve to the same Decimal value.
            std::string_view sv = text;
            size_t mantissa_end = sv.size();
            Int64 exponent = 0;
            if (auto e_pos = sv.find_first_of("eE"); e_pos != std::string_view::npos)
            {
                mantissa_end = e_pos;
                size_t i = e_pos + 1;
                const bool exp_negative = i < sv.size() && sv[i] == '-';
                if (i < sv.size() && (sv[i] == '+' || sv[i] == '-'))
                    ++i;
                /// The cap keeps `exponent` from overflowing on absurd input; such a scale is rejected below anyway.
                for (; i < sv.size() && exponent < 100000; ++i)
                    exponent = exponent * 10 + (sv[i] - '0');
                if (exp_negative)
                    exponent = -exponent;
            }

            Int64 fractional_digits = 0;
            if (auto dot_pos = sv.substr(0, mantissa_end).find('.'); dot_pos != std::string_view::npos)
                fractional_digits = static_cast<Int64>(mantissa_end - dot_pos - 1);

            const Int64 literal_scale = std::max<Int64>(0, fractional_digits - exponent);

            /// Guard against scale exceeding Decimal256 max precision.
            if (literal_scale <= static_cast<Int64>(DataTypeDecimal<Decimal256>::maxPrecision()))
                target_type = std::make_shared<DataTypeDecimal<Decimal256>>(
                    DataTypeDecimal<Decimal256>::maxPrecision(), static_cast<UInt32>(literal_scale));
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

    /// For Decimal targets, convert from string text directly (preserves precision).
    /// For other targets, resolve the NumberLiteral first (see NumberLiteral::toFloat64 for floats).
    Field parsed_field;
    if (isDecimal(*target_type))
        parsed_field = tryConvertFieldToType(Field(text), *target_type);
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
