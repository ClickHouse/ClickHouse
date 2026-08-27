#include <Interpreters/resolveNumberLiteral.h>
#include <Interpreters/convertFieldToType.h>

#include <DataTypes/DataTypeArray.h>
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

namespace
{

bool hasNestedNumberLiteral(const Field & field)
{
    auto any_of = [](const auto & container)
    {
        return std::any_of(container.begin(), container.end(), [](const Field & element) { return hasNestedNumberLiteral(element); });
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

    /// Nothing to resolve it against: keep the default.
    Field resolved = field.resolveNumberLiteral();
    return {resolved, applyVisitor(FieldToDataType(), resolved)};
}

}

bool fieldHasNumberLiteral(const Field & field)
{
    return hasNestedNumberLiteral(field);
}

std::pair<Field, DataTypePtr> resolveNumberLiteralSetElement(
    const Field & element, const DataTypePtr & left_type)
{
    if (!left_type || !hasNestedNumberLiteral(element))
        return {};

    auto reference = removeNullable(removeLowCardinality(left_type));

    if (element.getType() == Field::Types::Number)
    {
        if (!isNumber(*reference) && !isDecimal(*reference))
            return {};
        return resolveNumberLiteralForFunction(element.safeGet<NumberLiteral>().value, reference, /*is_comparison=*/ true);
    }

    return resolveNestedNumberLiteralsForComparison(element, reference);
}

std::pair<Field, DataTypePtr> resolveNestedNumberLiteralsForComparison(
    const Field & field, const DataTypePtr & reference_type)
{
    /// A bare literal goes through the scalar path.
    if (field.getType() != Field::Types::Tuple && field.getType() != Field::Types::Array)
        return {};
    if (!hasNestedNumberLiteral(field))
        return {};

    bool any_resolved = false;
    auto [resolved_field, resolved_type] = resolveNested(field, reference_type, any_resolved);
    if (!any_resolved || !resolved_type)
        return {};

    return {std::move(resolved_field), std::move(resolved_type)};
}

}
