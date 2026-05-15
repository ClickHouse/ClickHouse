#include <Interpreters/resolveNumberLiteral.h>
#include <Interpreters/convertFieldToType.h>

#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>


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
            /// For comparisons: use Decimal with the literal's own scale, parsed from text.
            /// Skip scientific notation (scale can't be derived from exponent form).
            bool has_exponent = text.find_first_of("eE") != String::npos;
            if (!has_exponent)
            {
                size_t literal_scale = 0;
                if (auto dot_pos = text.find('.'); dot_pos != String::npos)
                    literal_scale = text.size() - dot_pos - 1;

                /// Guard against scale exceeding Decimal256 max precision.
                if (literal_scale <= DataTypeDecimal<Decimal256>::maxPrecision())
                    target_type = std::make_shared<DataTypeDecimal<Decimal256>>(
                        DataTypeDecimal<Decimal256>::maxPrecision(), static_cast<UInt32>(literal_scale));
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

    /// For Decimal targets, convert from string text directly (preserves precision).
    /// For other targets, resolve the NumberLiteral first (uses strtod for floats,
    /// which handles subnormals and edge cases that readFloatText doesn't).
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
