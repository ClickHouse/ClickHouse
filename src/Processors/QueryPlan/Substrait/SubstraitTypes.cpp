#include <Processors/QueryPlan/Substrait/SubstraitTypes.h>

#if USE_SUBSTRAIT

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <fmt/format.h>
#include <google/protobuf/util/message_differencer.h>
#include <Common/Exception.h>


/// Each macro adds one class of the spec's taxonomy. Entries are (kind case, proto accessor, short name).
/// * Simple = types with no parameters, so the short name alone can construct them.
/// * Compound = parameterized. Simple + compound = built-in types.
/// * User-defined = render as "u!<name>" and resolve via the extension registry.
#define FOR_EACH_SUBSTRAIT_SIMPLE_TYPE_KIND(M) \
    M(kBool, bool_, "bool") \
    M(kI8, i8, "i8") \
    M(kI16, i16, "i16") \
    M(kI32, i32, "i32") \
    M(kI64, i64, "i64") \
    M(kFp32, fp32, "fp32") \
    M(kFp64, fp64, "fp64") \
    M(kString, string, "str") \
    M(kDate, date, "date") \
    M(kUuid, uuid, "uuid")

#define FOR_EACH_SUBSTRAIT_BUILTIN_TYPE_KIND(M) \
    FOR_EACH_SUBSTRAIT_SIMPLE_TYPE_KIND(M) \
    M(kFixedBinary, fixed_binary, "fbin") \
    M(kDecimal, decimal, "dec") \
    M(kList, list, "list") \
    M(kMap, map, "map") \
    M(kStruct, struct_, "struct")

#define FOR_EACH_SUBSTRAIT_TYPE_KIND(M) \
    FOR_EACH_SUBSTRAIT_BUILTIN_TYPE_KIND(M) \
    M(kUserDefined, user_defined, "u!")


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

substrait::Type_Nullability substraitNullabilityOf(bool nullable)
{
    return nullable ? substrait::Type_Nullability_NULLABILITY_NULLABLE : substrait::Type_Nullability_NULLABILITY_REQUIRED;
}

int substraitSignedIntRank(std::string_view short_name)
{
    if (short_name == "i8")
        return 1;
    if (short_name == "i16")
        return 2;
    if (short_name == "i32")
        return 3;
    if (short_name == "i64")
        return 4;
    return 0;
}

bool isSubstraitFloatShort(std::string_view short_name) { return short_name == "fp32" || short_name == "fp64"; }

String substraitUserDefinedShortName(std::string_view type_name) { return fmt::format("u!{}", type_name); }

std::string_view tryParseUserDefinedShortName(std::string_view short_name)
{
    if (short_name.starts_with("u!"))
        return short_name.substr(2);
    return {};
}

substrait::Type substraitUnsignedType(std::string_view name, bool nullable, SubstraitPlanExtensions & extensions)
{
    substrait::Type result;
    auto * user_defined = result.mutable_user_defined();
    user_defined->set_type_reference(extensions.internType(SubstraitExtensionURN::unsigned_integers, name));
    user_defined->set_nullability(substraitNullabilityOf(nullable));
    return result;
}

std::expected<substrait::Type, String> convertTypeToSubstrait(const DataTypePtr & type, SubstraitPlanExtensions & extensions)
{
    /// LowCardinality is an encoding, not a semantic type. Unwrapping is lossless.
    DataTypePtr nested = removeLowCardinality(type);
    const bool nullable = nested->isNullable();
    nested = removeNullable(nested);

    substrait::Type result;

    const auto simple = [&](std::string_view short_name) -> std::expected<substrait::Type, String>
    { return makeSimpleSubstraitType(short_name, nullable); };

    const auto unsigned_int = [&](std::string_view name) -> std::expected<substrait::Type, String>
    { return substraitUnsignedType(name, nullable, extensions); };

    if (isBool(nested))
        return simple("bool");

    WhichDataType which(nested);

    switch (which.idx)
    {
        case TypeIndex::Int8: return simple("i8");
        case TypeIndex::Int16: return simple("i16");
        case TypeIndex::Int32: return simple("i32");
        case TypeIndex::Int64: return simple("i64");
        case TypeIndex::UInt8: return unsigned_int("u8");
        case TypeIndex::UInt16: return unsigned_int("u16");
        case TypeIndex::UInt32: return unsigned_int("u32");
        case TypeIndex::UInt64: return unsigned_int("u64");
        case TypeIndex::Float32: return simple("fp32");
        case TypeIndex::Float64: return simple("fp64");
        case TypeIndex::String:
            /// String holds arbitrary bytes but Substrait `string` is always unicode. We map it anyway (as every other SQL engine I checked).
            return simple("str");
        case TypeIndex::Date:
        case TypeIndex::Date32: return simple("date");
        case TypeIndex::UUID: return simple("uuid");
        case TypeIndex::FixedString: {
            const auto & fixed = assert_cast<const DataTypeFixedString &>(*nested);
            auto * fixed_binary = result.mutable_fixed_binary();
            fixed_binary->set_length(static_cast<Int32>(fixed.getN()));
            fixed_binary->set_nullability(substraitNullabilityOf(nullable));
            return std::move(result);
        }
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128: {
            const auto precision = getDecimalPrecision(*nested);
            const auto scale = getDecimalScale(*nested);
            /// Substrait decimals cap at 38 digits, the Decimal128 maximum. The check matters only if Decimal256 joins this switch.
            if (precision > DecimalUtils::max_precision<Decimal128>)
                return blocked(fmt::format("type {} exceeds the maximum Substrait decimal precision of 38", type->getName()));
            auto * decimal = result.mutable_decimal();
            decimal->set_precision(static_cast<Int32>(precision));
            decimal->set_scale(static_cast<Int32>(scale));
            decimal->set_nullability(substraitNullabilityOf(nullable));
            return std::move(result);
        }
        case TypeIndex::Array: {
            const auto & array = assert_cast<const DataTypeArray &>(*nested);
            auto element = convertTypeToSubstrait(array.getNestedType(), extensions);
            if (!element)
                return std::unexpected(std::move(element.error()));
            auto * list = result.mutable_list();
            *list->mutable_type() = std::move(*element);
            list->set_nullability(substraitNullabilityOf(nullable));
            return std::move(result);
        }
        case TypeIndex::Map: {
            const auto & map = assert_cast<const DataTypeMap &>(*nested);
            auto key = convertTypeToSubstrait(map.getKeyType(), extensions);
            if (!key)
                return std::unexpected(std::move(key.error()));
            auto value = convertTypeToSubstrait(map.getValueType(), extensions);
            if (!value)
                return std::unexpected(std::move(value.error()));
            auto * map_type = result.mutable_map();
            *map_type->mutable_key() = std::move(*key);
            *map_type->mutable_value() = std::move(*value);
            map_type->set_nullability(substraitNullabilityOf(nullable));
            return std::move(result);
        }
        case TypeIndex::Tuple: {
            const auto & tuple = assert_cast<const DataTypeTuple &>(*nested);
            /// Substrait struct fields are anonymous. Only unnamed tuples map exactly.
            if (tuple.hasExplicitNames())
                return blocked(fmt::format("type {} has named elements that Substrait structs cannot represent", type->getName()));
            auto * struct_type = result.mutable_struct_();
            for (const auto & element_type : tuple.getElements())
            {
                auto element = convertTypeToSubstrait(element_type, extensions);
                if (!element)
                    return std::unexpected(std::move(element.error()));
                *struct_type->add_types() = std::move(*element);
            }
            struct_type->set_nullability(substraitNullabilityOf(nullable));
            return std::move(result);
        }
        default: return blocked(fmt::format("type {} has no exact Substrait representation", type->getName()));
    }
}

String substraitTypeShortName(const substrait::Type & type, const SubstraitPlanExtensions & extensions)
{
    switch (type.kind_case())
    {
        case substrait::Type::kUserDefined:
            return substraitUserDefinedShortName(extensions.typeNameByAnchor(type.user_defined().type_reference()));
#define M(KIND, ACCESSOR, SHORT_NAME) \
    case substrait::Type::KIND: return SHORT_NAME;
            FOR_EACH_SUBSTRAIT_BUILTIN_TYPE_KIND(M)
#undef M
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Substrait type kind {}", static_cast<int>(type.kind_case()));
    }
}

void setSubstraitTypeByShortName(substrait::Type & type, std::string_view short_name)
{
#define M(KIND, ACCESSOR, SHORT_NAME) \
    if (short_name == SHORT_NAME) \
    { \
        type.mutable_##ACCESSOR(); \
        return; \
    }
    FOR_EACH_SUBSTRAIT_SIMPLE_TYPE_KIND(M)
#undef M
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Type short name {} does not name a simple type", short_name);
}

substrait::Type makeSimpleSubstraitType(std::string_view short_name, bool nullable)
{
    substrait::Type type;
    setSubstraitTypeByShortName(type, short_name);
    setSubstraitTypeNullability(type, nullable);
    return type;
}

bool substraitTypesIdentical(const substrait::Type & lhs, const substrait::Type & rhs)
{
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

bool isSubstraitTypeNullable(const substrait::Type & type)
{
    switch (type.kind_case())
    {
#define M(KIND, ACCESSOR, SHORT_NAME) \
    case substrait::Type::KIND: return type.ACCESSOR().nullability() == substrait::Type_Nullability_NULLABILITY_NULLABLE;
        FOR_EACH_SUBSTRAIT_TYPE_KIND(M)
#undef M
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Substrait type kind {}", static_cast<int>(type.kind_case()));
    }
}

void setSubstraitTypeNullability(substrait::Type & type, bool nullable)
{
    const auto nullability = substraitNullabilityOf(nullable);
    switch (type.kind_case())
    {
#define M(KIND, ACCESSOR, SHORT_NAME) \
    case substrait::Type::KIND: type.mutable_##ACCESSOR()->set_nullability(nullability); break;
        FOR_EACH_SUBSTRAIT_TYPE_KIND(M)
#undef M
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected Substrait type kind {}", static_cast<int>(type.kind_case()));
    }
}

}

#endif
