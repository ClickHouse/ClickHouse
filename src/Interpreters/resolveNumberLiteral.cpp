#include <Interpreters/resolveNumberLiteral.h>
#include <Interpreters/convertFieldToType.h>

#include <Common/StringUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>

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

/// The precision a normalized decimal string needs: its digits without the sign, the point and the
/// leading zeroes of the integer part (`-0.0015` needs 4, `12.50` needs 4).
UInt64 decimalDigits(const String & normalized)
{
    UInt64 digits = 0;
    bool integer_part = true;
    for (char c : normalized)
    {
        if (c == '.')
            integer_part = false;
        else if (isNumericASCII(c) && !(integer_part && c == '0' && digits == 0))
            ++digits;
    }
    return digits;
}

}

std::pair<Field, DataTypePtr> resolveNumberLiteralForFunction(
    const String & text, const DataTypePtr & reference_type, bool is_comparison)
{
    const Field resolved = Field(NumberLiteral(text)).resolveNumberLiteral();
    auto default_type = applyVisitor(FieldToDataType(), resolved);

    DataTypePtr target_type = default_type;
    Field literal_value = resolved;
    /// Set when the literal is parsed into a Decimal from its (normalized) text instead of `literal_value`.
    String decimal_text;
    if (reference_type)
    {
        auto ref = removeNullable(reference_type);
        WhichDataType which_default(default_type);
        WhichDataType which_ref(ref);

        if (isDecimal(*ref))
        {
            /// Fold the exponent and drop insignificant trailing zeroes first, so different spellings
            /// of one value (`1.5e-3`, `0.0015`) resolve identically. A value that does not fit
            /// Decimal256 keeps the Float64 default.
            UInt32 scale = 0;
            if (normalizeDecimalLiteral(text, decimal_text, scale))
            {
                if (is_comparison)
                {
                    target_type = std::make_shared<DataTypeDecimal<Decimal256>>(DataTypeDecimal<Decimal256>::maxPrecision(), scale);
                }
                else
                {
                    /// Otherwise the literal is cast to the sibling's type, exactly as an explicit constant of
                    /// that type would be, and widened only where it does not fit: `1.5` next to a
                    /// `Decimal32(5)` is `Decimal32(5)`, `1.123` next to a `Decimal32(2)` is `Decimal32(3)`.
                    const UInt32 target_scale = std::max(scale, getDecimalScale(*ref));
                    const UInt64 precision = std::max<UInt64>(decimalDigits(decimal_text) + (target_scale - scale), getDecimalPrecision(*ref));
                    if (precision <= DataTypeDecimal<Decimal256>::maxPrecision())
                        target_type = createDecimal<DataTypeDecimal>(precision, target_scale);
                }
            }
        }
        else if ((which_default.isInt() || which_default.isUInt()) && which_ref.isFloat())
        {
            /// The integer spelling says nothing about the type: next to a float the literal is a float,
            /// so a value too large for UInt64 is read as one, rounding as a float literal does.
            literal_value = NumberLiteral(text).toFloat64();
            target_type = std::make_shared<DataTypeFloat64>();
        }
        else if ((which_default.isInt() && which_ref.isInt()) || (which_default.isUInt() && (which_ref.isUInt() || which_ref.isInt())))
        {
            if (default_type->getSizeOfValueInMemory() <= ref->getSizeOfValueInMemory())
                target_type = ref;
        }
        else if (which_default.isFloat() && which_ref.isFloat())
        {
            target_type = ref;
        }
    }

    Field parsed_field = isDecimal(*target_type)
        ? tryConvertFieldToType(Field(decimal_text), *target_type)
        : tryConvertFieldToType(literal_value, *target_type);

    if (parsed_field.isNull() && target_type != default_type)
    {
        target_type = default_type;
        parsed_field = tryConvertFieldToType(resolved, *default_type);
    }

    if (parsed_field.isNull())
        return {Field(), nullptr};
    return {parsed_field, target_type};
}

bool fieldHasNumberLiteral(const Field & field)
{
    auto any_of = [](const auto & container)
    {
        return std::any_of(container.begin(), container.end(), fieldHasNumberLiteral);
    };

    switch (field.getType())
    {
        case Field::Types::Number: return true;
        case Field::Types::Array: return any_of(field.safeGet<Array>());
        case Field::Types::Tuple: return any_of(field.safeGet<Tuple>());
        case Field::Types::Map: return any_of(field.safeGet<Map>());
        default: return false;
    }
}

namespace
{

/// `any_resolved` is set when the reference type decided an element's type. A null type means give up.
std::pair<Field, DataTypePtr> resolveNested(const Field & field, const DataTypePtr & reference_type, bool & any_resolved)
{
    auto reference = reference_type ? removeNullable(removeLowCardinality(reference_type)) : nullptr;

    if (field.getType() == Field::Types::Number)
    {
        const String & text = field.safeGet<NumberLiteral>().value;
        if (reference && (isNumber(*reference) || isDecimal(*reference)))
        {
            auto [parsed_field, target_type] = resolveNumberLiteralForFunction(text, reference, /*is_comparison=*/ true);
            if (target_type)
            {
                any_resolved = true;
                return {std::move(parsed_field), std::move(target_type)};
            }
        }
    }
    else if (field.getType() == Field::Types::Tuple)
    {
        const auto & elements = field.safeGet<Tuple>();
        const auto * reference_tuple = reference ? typeid_cast<const DataTypeTuple *>(reference.get()) : nullptr;
        if (reference_tuple && reference_tuple->getElements().size() != elements.size())
            reference_tuple = nullptr;

        Tuple resolved_elements;
        DataTypes resolved_types;
        resolved_elements.reserve(elements.size());
        resolved_types.reserve(elements.size());
        for (size_t i = 0; i < elements.size(); ++i)
        {
            auto [resolved, type] = resolveNested(elements[i], reference_tuple ? reference_tuple->getElement(i) : nullptr, any_resolved);
            if (!type)
                return {};
            resolved_elements.push_back(std::move(resolved));
            resolved_types.push_back(std::move(type));
        }
        return {Field(std::move(resolved_elements)), std::make_shared<DataTypeTuple>(std::move(resolved_types))};
    }
    else if (field.getType() == Field::Types::Array)
    {
        const auto & elements = field.safeGet<Array>();
        const auto * reference_array = reference ? typeid_cast<const DataTypeArray *>(reference.get()) : nullptr;
        DataTypePtr element_reference = reference_array ? reference_array->getNestedType() : nullptr;

        Array resolved_elements;
        DataTypes resolved_types;
        resolved_elements.reserve(elements.size());
        resolved_types.reserve(elements.size());
        for (const auto & element : elements)
        {
            auto [resolved, type] = resolveNested(element, element_reference, any_resolved);
            if (!type)
                return {};
            resolved_elements.push_back(std::move(resolved));
            resolved_types.push_back(std::move(type));
        }

        /// An array holds one type, and different spellings resolve to different Decimal scales.
        auto element_type = resolved_types.empty() ? element_reference : tryGetLeastSupertype(resolved_types);
        if (!element_type)
            return {};
        for (auto & element : resolved_elements)
        {
            Field converted = tryConvertFieldToType(element, *element_type);
            if (converted.isNull() && !element.isNull())
                return {};
            element = std::move(converted);
        }
        return {Field(std::move(resolved_elements)), std::make_shared<DataTypeArray>(element_type)};
    }

    else if (field.getType() == Field::Types::Map)
    {
        /// A map field is a list of key/value pairs, each a two-element tuple.
        const auto & elements = field.safeGet<Map>();
        const auto * reference_map = reference ? typeid_cast<const DataTypeMap *>(reference.get()) : nullptr;
        DataTypePtr pair_reference;
        if (reference_map)
            pair_reference = std::make_shared<DataTypeTuple>(
                DataTypes{reference_map->getKeyType(), reference_map->getValueType()});

        Map resolved_elements;
        DataTypes resolved_types;
        resolved_elements.reserve(elements.size());
        resolved_types.reserve(elements.size());
        for (const auto & element : elements)
        {
            auto [resolved, type] = resolveNested(element, pair_reference, any_resolved);
            if (!type)
                return {};
            resolved_elements.push_back(std::move(resolved));
            resolved_types.push_back(std::move(type));
        }

        /// A map holds one key type and one value type, so the pairs have to meet in a common one.
        auto pair_type = resolved_types.empty() ? pair_reference : tryGetLeastSupertype(resolved_types);
        const auto * pair_tuple = pair_type ? typeid_cast<const DataTypeTuple *>(pair_type.get()) : nullptr;
        if (!pair_tuple || pair_tuple->getElements().size() != 2)
            return {};
        for (auto & element : resolved_elements)
        {
            Field converted = tryConvertFieldToType(element, *pair_type);
            if (converted.isNull() && !element.isNull())
                return {};
            element = std::move(converted);
        }
        return {Field(std::move(resolved_elements)),
                std::make_shared<DataTypeMap>(pair_tuple->getElement(0), pair_tuple->getElement(1))};
    }

    /// Nothing to resolve it against: keep the default.
    Field resolved = field.resolveNumberLiteral();
    return {resolved, applyVisitor(FieldToDataType(), resolved)};
}

}

std::pair<Field, DataTypePtr> resolveNumberLiteralSetElement(const Field & element, const DataTypePtr & left_type)
{
    if (!left_type || !fieldHasNumberLiteral(element))
        return {};

    bool any_resolved = false;
    auto [resolved_field, resolved_type] = resolveNested(element, left_type, any_resolved);
    if (!any_resolved || !resolved_type)
        return {};
    return {std::move(resolved_field), std::move(resolved_type)};
}

std::pair<Field, DataTypePtr> resolveNestedNumberLiteralsForComparison(const Field & field, const DataTypePtr & reference_type)
{
    /// A bare literal goes through the scalar path.
    const auto type = field.getType();
    if (type != Field::Types::Tuple && type != Field::Types::Array && type != Field::Types::Map)
        return {};
    return resolveNumberLiteralSetElement(field, reference_type);
}

}
