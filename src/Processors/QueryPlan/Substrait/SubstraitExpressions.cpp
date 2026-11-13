#include <Processors/QueryPlan/Substrait/SubstraitExpressions.h>

#if USE_SUBSTRAIT

#include <Columns/ColumnConst.h>
#include <Core/AccurateComparison.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/Substrait/SubstraitTypes.h>
#include <base/defines.h>
#include <base/unaligned.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>
#include <Common/isValidUTF8.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

substrait::Expression makeColumnReference(size_t position)
{
    /// A row counts as a struct whose members are the columns, hence struct_field. The empty root_reference marks the input row as the base.
    substrait::Expression expression;
    auto * selection = expression.mutable_selection();
    selection->mutable_direct_reference()->mutable_struct_field()->set_field(static_cast<Int32>(position));
    selection->mutable_root_reference();
    return expression;
}


namespace
{

bool isConstantNode(const ActionsDAG::Node & node) { return node.column && isColumnConst(*node.column) && node.is_deterministic_constant; }

/// Follows aliases down to the underlying node. An alias only carries a name, and Substrait refers to columns by position.
const ActionsDAG::Node & unalias(const ActionsDAG::Node & node)
{
    const auto * current = &node;
    while (current->type == ActionsDAG::ActionType::ALIAS)
        current = current->children.at(0);
    return *current;
}

/// How arguments of function are brought to the types of a concrete Substrait overload.
enum class Coercion : uint8_t
{
    None, /// Arguments are used with their own types.
    ToResultType, /// All arguments are brought to the ClickHouse result type (arithmetic).
    ToCommonType, /// All arguments are brought to one common type (comparisons).
};

/// One overload of a Substrait function ("add:i64_i64" takes {"i64", "i64"} and returns "i64").
struct FunctionOverload
{
    std::vector<std::string_view> arg_types;
    std::string_view return_type;
    std::string_view urn; /// Extension that declares this function.
    FunctionOptions options = {};
};

/// One ClickHouse function's Substrait counterpart and the rules that control how we emit a call.
struct MappedFunction
{
    std::string_view substrait_name;
    Coercion coercion = Coercion::None;
    bool variadic = false;
    bool never_null = false;
    bool negate_result = false; /// For negated ClickHouse functions with no negated Substrait form, as in notLike -> not(like(...)).
    std::vector<FunctionOverload> overloads;
};

/// How ClickHouse integer/float maps to Substrait.
FunctionOptions intArithOptions() { return {{"overflow", "SILENT"}}; }
FunctionOptions fpArithOptions() { return {{"rounding", "TIE_TO_EVEN"}}; }

/// The shared overload set of add, subtract and multiply. One overload per numeric type.
std::vector<FunctionOverload> sameTypeArithmeticOverloads()
{
    using namespace SubstraitExtensionURN;
    std::vector<FunctionOverload> overloads;
    for (std::string_view t : {"i8", "i16", "i32", "i64"})
        overloads.push_back({{t, t}, t, arithmetic, intArithOptions()});
    for (std::string_view t : {"fp32", "fp64"})
        overloads.push_back({{t, t}, t, arithmetic, fpArithOptions()});
    for (std::string_view t : {"u!u8", "u!u16", "u!u32", "u!u64"})
        overloads.push_back({{t, t}, t, unsigned_integers, intArithOptions()});
    return overloads;
}

/// The ClickHouse functions we can translate, keyed by ClickHouse name. New functions go here. Only call via functionTable.
std::unordered_map<std::string_view, MappedFunction> buildFunctionTable()
{
    using namespace SubstraitExtensionURN;

    std::unordered_map<std::string_view, MappedFunction> table;

    const auto add_function = [&](std::string_view ch_name,
                                  std::string_view substrait_name,
                                  std::vector<FunctionOverload> overloads,
                                  Coercion coercion = Coercion::None,
                                  bool variadic = false) -> MappedFunction &
    {
        auto & entry = table[ch_name];
        entry = {.substrait_name = substrait_name, .coercion = coercion, .variadic = variadic, .overloads = std::move(overloads)};
        return entry;
    };

    add_function("plus", "add", sameTypeArithmeticOverloads(), Coercion::ToResultType);
    add_function("minus", "subtract", sameTypeArithmeticOverloads(), Coercion::ToResultType);
    add_function("multiply", "multiply", sameTypeArithmeticOverloads(), Coercion::ToResultType);

    const FunctionOptions fp_divide_options = {{"rounding", "TIE_TO_EVEN"}, {"on_domain_error", "NAN"}, {"on_division_by_zero", "IEEE"}};
    add_function(
        "divide",
        "divide",
        {{{"fp32", "fp32"}, "fp32", arithmetic, fp_divide_options}, {{"fp64", "fp64"}, "fp64", arithmetic, fp_divide_options}},
        Coercion::ToResultType);

    const FunctionOptions modulus_options = {{"division_type", "TRUNCATE"}, {"overflow", "SILENT"}, {"on_domain_error", "ERROR"}};
    std::vector<FunctionOverload> modulus_overloads;
    for (std::string_view ty : {"i8", "i16", "i32", "i64"})
        modulus_overloads.push_back({{ty, ty}, ty, arithmetic, modulus_options});
    add_function("modulo", "modulus", std::move(modulus_overloads), Coercion::ToResultType);

    std::vector<FunctionOverload> negate_overloads;
    for (std::string_view ty : {"i8", "i16", "i32", "i64"})
        negate_overloads.push_back({{ty}, ty, arithmetic, intArithOptions()});
    for (std::string_view ty : {"fp32", "fp64"})
        negate_overloads.push_back({{ty}, ty, arithmetic});
    add_function("negate", "negate", std::move(negate_overloads));

    /// abs maps only for floats. ClickHouse types abs(IntN) as UIntN, which doesn't match the same-type Substrait overloads.
    add_function("abs", "abs", {{{"fp32"}, "fp32", arithmetic}, {{"fp64"}, "fp64", arithmetic}});

    /// Substrait declares comparisons as `any1, any1`, any type but the same one for both arguments. The coercion makes them the same.
    const std::vector<FunctionOverload> compare_overload = {{{"any", "any"}, "bool", comparison}};
    add_function("equals", "equal", compare_overload, Coercion::ToCommonType);
    add_function("notEquals", "not_equal", compare_overload, Coercion::ToCommonType);
    add_function("less", "lt", compare_overload, Coercion::ToCommonType);
    add_function("greater", "gt", compare_overload, Coercion::ToCommonType);
    add_function("lessOrEquals", "lte", compare_overload, Coercion::ToCommonType);
    add_function("greaterOrEquals", "gte", compare_overload, Coercion::ToCommonType);
    add_function("isNull", "is_null", {{{"any"}, "bool", comparison}}).never_null = true;
    add_function("isNotNull", "is_not_null", {{{"any"}, "bool", comparison}}).never_null = true;

    const std::vector<FunctionOverload> boolean_overload = {{{"bool"}, "bool", boolean}};
    add_function("and", "and", boolean_overload, Coercion::None, /*variadic=*/true);
    add_function("or", "or", boolean_overload, Coercion::None, /*variadic=*/true);
    add_function("not", "not", boolean_overload);
    add_function("xor", "xor", {{{"bool", "bool"}, "bool", boolean}});

    const std::vector<FunctionOverload> like_sensitive = {{{"str", "str"}, "bool", string, {{"case_sensitivity", "CASE_SENSITIVE"}}}};
    const std::vector<FunctionOverload> like_insensitive = {{{"str", "str"}, "bool", string, {{"case_sensitivity", "CASE_INSENSITIVE"}}}};
    add_function("like", "like", like_sensitive);
    add_function("ilike", "like", like_insensitive);
    add_function("notLike", "like", like_sensitive).negate_result = true;
    add_function("notILike", "like", like_insensitive).negate_result = true;

    add_function("concat", "concat", {{{"str"}, "str", string, {{"null_handling", "ACCEPT_NULLS"}}}}, Coercion::None, /*variadic=*/true);

    add_function("lower", "lower", {{{"str"}, "str", string, {{"char_set", "ASCII_ONLY"}}}});
    add_function("upper", "upper", {{{"str"}, "str", string, {{"char_set", "ASCII_ONLY"}}}});
    add_function("lowerUTF8", "lower", {{{"str"}, "str", string, {{"char_set", "UTF8"}}}});
    add_function("upperUTF8", "upper", {{{"str"}, "str", string, {{"char_set", "UTF8"}}}});

    add_function("length", "octet_length", {{{"str"}, "i64", string}});
    add_function("lengthUTF8", "char_length", {{{"str"}, "i64", string}});
    add_function("startsWith", "starts_with", {{{"str", "str"}, "bool", string, {{"case_sensitivity", "CASE_SENSITIVE"}}}});
    add_function("endsWith", "ends_with", {{{"str", "str"}, "bool", string, {{"case_sensitivity", "CASE_SENSITIVE"}}}});

    add_function(
        "substring",
        "substring",
        {{{"str", "i32", "i32"}, "str", string, {{"negative_start", "WRAP_FROM_END"}}},
         {{"str", "i32"}, "str", string, {{"negative_start", "WRAP_FROM_END"}}}});

    add_function("floor", "floor", {{{"fp32"}, "fp32", rounding}, {{"fp64"}, "fp64", rounding}});
    add_function("ceil", "ceil", {{{"fp32"}, "fp32", rounding}, {{"fp64"}, "fp64", rounding}});

    add_function("sqrt", "sqrt", {{{"fp64"}, "fp64", arithmetic, {{"rounding", "TIE_TO_EVEN"}, {"on_domain_error", "NAN"}}}});
    add_function("exp", "exp", {{{"fp64"}, "fp64", arithmetic, fpArithOptions()}});
    const FunctionOptions log_options = {{"rounding", "TIE_TO_EVEN"}, {"on_domain_error", "NAN"}, {"on_log_zero", "MINUS_INFINITY"}};
    add_function("log", "ln", {{{"fp64"}, "fp64", logarithmic, log_options}});
    add_function("log2", "log2", {{{"fp64"}, "fp64", logarithmic, log_options}});
    add_function("log10", "log10", {{{"fp64"}, "fp64", logarithmic, log_options}});
    for (std::string_view trig : {"sin", "cos", "tan"})
        add_function(trig, trig, {{{"fp32"}, "fp32", arithmetic, fpArithOptions()}, {{"fp64"}, "fp64", arithmetic, fpArithOptions()}});

    /// Overload matching reads only `arg_types.front`, so a variadic overload declares its one repeated argument type once.
    for ([[maybe_unused]] const auto & [ch_name, mapped] : table)
        chassert(!mapped.variadic || std::ranges::all_of(mapped.overloads, [](const auto & o) { return o.arg_types.size() == 1; }));

    return table;
}

const std::unordered_map<std::string_view, MappedFunction> & functionTable()
{
    static const std::unordered_map<std::string_view, MappedFunction> table = buildFunctionTable();
    return table;
}

/// The value of an integer constant, signed or unsigned. Empty for other field types.
std::optional<Int128> tryGetIntegerField(const Field & field)
{
    if (field.getType() == Field::Types::Int64)
        return field.safeGet<Int64>();
    if (field.getType() == Field::Types::UInt64)
        return field.safeGet<UInt64>();
    return {};
}

template <typename To>
bool fitsExactly(Int128 value)
{
    To result;
    return accurate::convertNumeric<Int128, To>(value, result);
}

bool integerFits(Int128 value, std::string_view short_name)
{
    if (short_name == "i8")
        return fitsExactly<Int8>(value);
    if (short_name == "i16")
        return fitsExactly<Int16>(value);
    if (short_name == "i32")
        return fitsExactly<Int32>(value);
    if (short_name == "i64")
        return fitsExactly<Int64>(value);
    if (short_name == "u!u8")
        return fitsExactly<UInt8>(value);
    if (short_name == "u!u16")
        return fitsExactly<UInt16>(value);
    if (short_name == "u!u32")
        return fitsExactly<UInt32>(value);
    if (short_name == "u!u64")
        return fitsExactly<UInt64>(value);
    if (short_name == "fp32")
        return fitsExactly<Float32>(value);
    if (short_name == "fp64")
        return fitsExactly<Float64>(value);
    return false;
}

/// A constant converted into a Substrait literal with its type.
struct ConvertedLiteral
{
    substrait::Expression_Literal literal;
    substrait::Type type;
};

ConvertedLiteral encodeIntegerLiteral(Int128 value, std::string_view short_name, bool nullable, SubstraitPlanExtensions & extensions)
{
    ConvertedLiteral out;

    /// Unsigned integers are user-defined types whose declared structure is one string field holding the decimal value.
    if (const auto type_name = tryParseUserDefinedShortName(short_name); !type_name.empty())
    {
        out.type = substraitUnsignedType(type_name, nullable, extensions);
        auto * user_defined = out.literal.mutable_user_defined();
        user_defined->set_type_reference(out.type.user_defined().type_reference());
        user_defined->mutable_struct_()->add_fields()->set_string(toString(static_cast<UInt64>(value)));
        return out;
    }

    if (short_name == "i8")
        out.literal.set_i8(static_cast<Int32>(value));
    else if (short_name == "i16")
        out.literal.set_i16(static_cast<Int32>(value));
    else if (short_name == "i32")
        out.literal.set_i32(static_cast<Int32>(value));
    else if (short_name == "i64")
        out.literal.set_i64(static_cast<Int64>(value));
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected integer short name {}", short_name);
    out.type = makeSimpleSubstraitType(short_name, nullable);
    return out;
}

/// A float literal of the given short name ("fp32"/"fp64").
ConvertedLiteral encodeFloatLiteral(Float64 value, std::string_view short_name, bool nullable)
{
    ConvertedLiteral out;
    if (short_name == "fp32")
        out.literal.set_fp32(static_cast<Float32>(value));
    else if (short_name == "fp64")
        out.literal.set_fp64(value);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected float short name {}", short_name);
    out.type = makeSimpleSubstraitType(short_name, nullable);
    return out;
}

/// A cast of `input` to `type` that throws on failure, as ClickHouse casts do. The alternative returns null.
substrait::Expression makeCast(substrait::Type type, substrait::Expression input)
{
    substrait::Expression result;
    auto * cast = result.mutable_cast();
    *cast->mutable_type() = std::move(type);
    *cast->mutable_input() = std::move(input);
    cast->set_failure_behavior(substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
    return result;
}

/// A call of the function `signature` from `urn` that returns `output_type`.
substrait::Expression makeScalarCall(
    std::string_view urn,
    std::string_view signature,
    std::vector<SubstraitExpressionConverter::ConvertedExpression> && arguments,
    const substrait::Type & output_type,
    SubstraitPlanExtensions & extensions,
    const FunctionOptions & options = {})
{
    substrait::Expression result;
    auto * function = result.mutable_scalar_function();
    function->set_function_reference(extensions.internFunction(urn, signature));
    for (auto & argument : arguments)
        *function->add_arguments()->mutable_value() = std::move(argument.expression);
    addFunctionOptions(options, *function);
    *function->mutable_output_type() = output_type;
    return result;
}

/// Converts a constant into a Substrait literal. A non-empty `desired_short_name` encodes the value in that type instead when it fits exactly.
/// For ClickHouse widening. `a + 1` over Int32 computes in Int64, so `1` re-encodes as i64.
std::expected<ConvertedLiteral, SubstraitBlockingReason> convertFieldToLiteral(
    const Field & field, const DataTypePtr & ch_type, std::string_view desired_short_name, SubstraitPlanExtensions & extensions)
{
    ConvertedLiteral out;

    if (field.isNull())
    {
        auto type = convertTypeToSubstrait(ch_type, extensions);
        if (!type)
            return std::unexpected(std::move(type.error()));
        out.type = std::move(*type);
        setSubstraitTypeNullability(out.type, true);
        out.literal.mutable_null()->CopyFrom(out.type);
        return out;
    }

    auto natural_type = convertTypeToSubstrait(ch_type, extensions);
    if (!natural_type)
        return std::unexpected(std::move(natural_type.error()));
    out.type = std::move(*natural_type);

    const bool nullable = isSubstraitTypeNullable(out.type);
    /// For `1` in `a + 1` over Int32 the source is "u!u8" (ClickHouse types `1` as UInt8) and the target is "i64".
    const String source_short = substraitTypeShortName(out.type, extensions);
    String target_short{desired_short_name};
    if (target_short.empty())
        target_short = source_short;

    const auto unsupported_target
        = [&] { return blocked(fmt::format("constant of type {} cannot be encoded as {}", ch_type->getName(), target_short)); };

    switch (out.type.kind_case())
    {
        case substrait::Type::kBool:
            if (target_short != source_short)
                return unsupported_target();
            out.literal.set_boolean(field.safeGet<UInt64>() != 0);
            break;
        case substrait::Type::kI8:
        case substrait::Type::kI16:
        case substrait::Type::kI32:
        case substrait::Type::kI64:
        case substrait::Type::kUserDefined: /// unsigned integers
        {
            const auto value = tryGetIntegerField(field);
            if (!value)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Constant of integer type {} holds a non-integer field", ch_type->getName());
            if (!integerFits(*value, target_short))
                return blocked(fmt::format("constant {} is not exactly representable as {}", toString(*value), target_short));
            if (isSubstraitFloatShort(target_short))
            {
                out = encodeFloatLiteral(static_cast<Float64>(*value), target_short, nullable);
                break;
            }
            out = encodeIntegerLiteral(*value, target_short, nullable, extensions);
            break;
        }
        case substrait::Type::kFp32:
        case substrait::Type::kFp64:
            /// A float keeps its type or widens to fp64.
            if (target_short != source_short && target_short != "fp64")
                return unsupported_target();
            out = encodeFloatLiteral(field.safeGet<Float64>(), target_short, nullable);
            break;
        case substrait::Type::kString: {
            if (target_short != source_short)
                return unsupported_target();
            const auto & value = field.safeGet<String>();
            /// Protobuf string fields require valid UTF-8.
            if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
                return blocked("string constant is not valid UTF-8");
            out.literal.set_string(value);
            break;
        }
        case substrait::Type::kDate: {
            if (target_short != source_short)
                return unsupported_target();
            const auto days = tryGetIntegerField(field);
            if (!days)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Constant of date type {} holds a non-integer field", ch_type->getName());
            out.literal.set_date(static_cast<Int32>(*days));
            break;
        }
        case substrait::Type::kDecimal: {
            if (target_short != source_short)
                return unsupported_target();
            Int128 unscaled;
            switch (field.getType())
            {
                case Field::Types::Decimal32: unscaled = field.safeGet<DecimalField<Decimal32>>().getValue().value; break;
                case Field::Types::Decimal64: unscaled = field.safeGet<DecimalField<Decimal64>>().getValue().value; break;
                case Field::Types::Decimal128: unscaled = field.safeGet<DecimalField<Decimal128>>().getValue().value; break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Constant of decimal type {} holds a non-decimal field", ch_type->getName());
            }
            auto * decimal = out.literal.mutable_decimal();
            /// Protobuf has no 128-bit integer, so Literal.Decimal stores the unscaled value in the byte layout of Int128.
            String bytes(sizeof(Int128), '\0');
            unalignedStoreLittleEndian<Int128>(bytes.data(), unscaled);
            decimal->set_value(std::move(bytes));
            decimal->set_precision(out.type.decimal().precision());
            decimal->set_scale(out.type.decimal().scale());
            break;
        }
        default: return blocked(fmt::format("constant of type {} is not representable as a Substrait literal", ch_type->getName()));
    }

    if (nullable && !out.literal.has_null())
        out.literal.set_nullable(true);

    return out;
}

}

std::expected<ConvertedRelation, SubstraitBlockingReason>
SubstraitExpressionConverter::convertProjection(const ActionsDAG & dag, const Block & output_header, substrait::Rel child)
{
    /// In a header [plus(a, b), c] the DAG computes the first, so `node` points at it.
    /// The input already has `c`, so `node` is null and `passthrough_position` says where it sits.
    struct ColumnSource
    {
        const ActionsDAG::Node * node = nullptr;
        size_t passthrough_position = 0;
    };

    std::unordered_map<std::string_view, const ActionsDAG::Node *> output_nodes;
    for (const auto * output : dag.getOutputs())
        output_nodes.try_emplace(output->result_name, output);

    std::vector<ColumnSource> sources;
    sources.reserve(output_header.columns());

    for (const auto & column : output_header)
    {
        if (const auto it = output_nodes.find(column.name); it != output_nodes.end())
            sources.push_back({.node = it->second});
        else if (const auto position = input_schema.positionByName(column.name))
            sources.push_back({.passthrough_position = *position});
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Output column {} is neither produced by the expression nor passed through", column.name);
    }

    const auto is_identity = [&]
    {
        if (sources.size() != input_schema.size())
            return false;
        for (size_t i = 0; i < sources.size(); ++i)
        {
            if (!sources[i].node)
            {
                if (sources[i].passthrough_position != i)
                    return false;
                continue;
            }
            const auto & underlying = unalias(*sources[i].node);
            if (underlying.type != ActionsDAG::ActionType::INPUT || input_schema.positionByName(underlying.result_name) != i)
                return false;
        }
        return true;
    };

    ConvertedRelation out;

    if (is_identity())
    {
        /// Pure pass-through or rename.
        for (size_t i = 0; i < sources.size(); ++i)
            out.schema.append(output_header.getByPosition(i).name, input_schema.columns[i].type);
        out.rel = std::move(child);
        return out;
    }

    substrait::Rel project_rel;
    auto * project = project_rel.mutable_project();
    auto * emit = project->mutable_common()->mutable_emit();
    for (size_t i = 0; i < sources.size(); ++i)
    {
        substrait::Expression expression;
        substrait::Type type;
        if (sources[i].node)
        {
            auto converted = convert(*sources[i].node);
            if (!converted)
                return std::unexpected(std::move(converted.error()));
            expression = std::move(converted->expression);
            type = std::move(converted->type);
        }
        else
        {
            expression = makeColumnReference(sources[i].passthrough_position);
            type = input_schema.columns[sources[i].passthrough_position].type;
        }
        *project->add_expressions() = std::move(expression);

        emit->add_output_mapping(static_cast<Int32>(input_schema.size() + i));
        out.schema.append(output_header.getByPosition(i).name, std::move(type));
    }

    *project->mutable_input() = std::move(child);
    out.rel = std::move(project_rel);
    return out;
}

std::expected<SubstraitExpressionConverter::ConvertedExpression, SubstraitBlockingReason>
SubstraitExpressionConverter::convert(const ActionsDAG::Node & node)
{
    if (const auto it = converted_nodes.find(&node); it != converted_nodes.end())
        return it->second;

    auto converted = convertNodeImpl(node);
    if (converted)
        converted_nodes.emplace(&node, *converted);
    /// The innermost failing node gets here first. The nodes above it keep its expression.
    else if (converted.error().expression.empty())
        converted.error().expression = node.result_name;
    return converted;
}

std::expected<SubstraitExpressionConverter::ConvertedExpression, SubstraitBlockingReason>
SubstraitExpressionConverter::convertNodeImpl(const ActionsDAG::Node & node)
{
    const auto constant_to_literal = [&]() -> std::expected<ConvertedExpression, SubstraitBlockingReason>
    {
        auto literal = convertFieldToLiteral((*node.column)[0], node.result_type, {}, extensions);
        if (!literal)
            return std::unexpected(std::move(literal.error()));
        ConvertedExpression out;
        out.node = &node;
        *out.expression.mutable_literal() = std::move(literal->literal);
        out.type = std::move(literal->type);
        return out;
    };

    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT: {
            const auto position = input_schema.positionByName(node.result_name);
            if (!position)
                return blocked("input column is not present in the child schema");
            ConvertedExpression out;
            out.node = &node;
            out.expression = makeColumnReference(*position);
            out.type = input_schema.columns[*position].type;
            return out;
        }
        case ActionsDAG::ActionType::COLUMN: return constant_to_literal(); /// ColumnConst
        case ActionsDAG::ActionType::ALIAS: return convert(*node.children.at(0));
        case ActionsDAG::ActionType::FUNCTION:
            /// A constant-folded deterministic function is emitted as its value.
            if (isConstantNode(node))
                return constant_to_literal();
            return convertFunction(node);
        case ActionsDAG::ActionType::ARRAY_JOIN: return blocked("ARRAY JOIN has no Substrait representation");
        case ActionsDAG::ActionType::PLACEHOLDER: return blocked("correlated columns have no Substrait representation");
    }
}

std::expected<void, SubstraitBlockingReason>
SubstraitExpressionConverter::coerceArgument(ConvertedExpression & arg, std::string_view desired_short_name)
{
    const auto current_short = substraitTypeShortName(arg.type, extensions);
    if (current_short == desired_short_name)
        return {};

    const bool nullable = isSubstraitTypeNullable(arg.type);

    /// Fitting constants re-encode as literals of the target type.
    /// TODO: An INPUT over a ColumnConst takes the cast path. Its value is on the node, so it could re-encode too.
    if (isConstantNode(*arg.node) && arg.expression.has_literal())
    {
        auto literal = convertFieldToLiteral((*arg.node->column)[0], arg.node->result_type, desired_short_name, extensions);
        if (!literal)
            return std::unexpected(std::move(literal.error()));
        *arg.expression.mutable_literal() = std::move(literal->literal);
        arg.type = std::move(literal->type);
        return {};
    }

    /// TODO: Emit the conversion function once the unsigned extension gets one (https://github.com/substrait-io/substrait/issues/1289).
    if (!tryParseUserDefinedShortName(desired_short_name).empty())
        return blocked(
            fmt::format(
                "no coercion from {} to {} because the unsigned extension types have no conversions in Substrait",
                current_short,
                desired_short_name));

    const auto current_rank = substraitSignedIntRank(current_short);
    const auto desired_rank = substraitSignedIntRank(desired_short_name);
    const bool int_widening = current_rank != 0 && desired_rank != 0 && current_rank < desired_rank;
    const bool fp_widening = current_short == "fp32" && desired_short_name == "fp64";
    if (!int_widening && !fp_widening)
        return blocked(fmt::format("no exact coercion from {} to {}", current_short, desired_short_name));

    substrait::Type target_type = makeSimpleSubstraitType(desired_short_name, nullable);
    arg.expression = makeCast(target_type, std::move(arg.expression));
    arg.type = std::move(target_type);
    return {};
}

std::expected<SubstraitExpressionConverter::ConvertedExpression, SubstraitBlockingReason>
SubstraitExpressionConverter::convertFunction(const ActionsDAG::Node & node)
{
    const String & name = node.function_base->getName();

    const auto & table = functionTable();
    const auto it = table.find(name);
    if (it == table.end())
        return blocked(fmt::format("function {} is not mapped to Substrait", name));

    const MappedFunction & mapping = it->second;

    std::vector<ConvertedExpression> args;
    args.reserve(node.children.size());
    for (const auto * child : node.children)
    {
        auto arg = convert(*child);
        if (!arg)
            return std::unexpected(std::move(arg.error()));
        args.push_back(std::move(*arg));
    }

    if (args.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} has no arguments, but every mapped function takes at least one", name);

    /// Convert the ClickHouse result type to Substrait, remember result.
    std::optional<substrait::Type> ch_result_type;
    String ch_result_short;
    const auto result_short = [&]() -> std::expected<std::string_view, SubstraitBlockingReason>
    {
        if (!ch_result_type)
        {
            auto converted_type = convertTypeToSubstrait(node.result_type, extensions);
            if (!converted_type)
                return std::unexpected(std::move(converted_type.error()));
            ch_result_type = std::move(*converted_type);
            ch_result_short = substraitTypeShortName(*ch_result_type, extensions);
        }
        return ch_result_short;
    };

    /// Coerce the arguments to one type per the mapping's rule.
    String coercion_target;
    if (mapping.coercion == Coercion::ToResultType)
    {
        auto target = result_short();
        if (!target)
            return std::unexpected(std::move(target.error()));
        coercion_target = *target;

        /// ClickHouse can type a result narrower than the arguments (`Int64 % UInt8` is Int16), but Substrait arithmetic is same-type.
        /// Raise the target to the widest argument and cast the result down later.
        if (substraitSignedIntRank(coercion_target) != 0)
        {
            for (const auto & arg : args)
            {
                const auto arg_short = substraitTypeShortName(arg.type, extensions);
                if (substraitSignedIntRank(arg_short) > substraitSignedIntRank(coercion_target))
                    coercion_target = arg_short;
            }
        }
    }
    else if (mapping.coercion == Coercion::ToCommonType)
    {
        /// ClickHouse compares mixed types natively, Substrait can't, so we invent casts to one common type and admit only value-preserving ones.
        /// The pick is the widest type among the non-constant arguments.
        /// Constants do not participate as they can be casted to any type that holds their value, while a column needs a real cast.
        std::vector<String> non_constant_shorts;
        for (const auto & arg : args)
            if (!isConstantNode(*arg.node))
                non_constant_shorts.push_back(substraitTypeShortName(arg.type, extensions));

        if (non_constant_shorts.empty())
            coercion_target = substraitTypeShortName(args.front().type, extensions);
        else if (std::ranges::all_of(non_constant_shorts, [&](const auto & s) { return s == non_constant_shorts.front(); }))
            coercion_target = non_constant_shorts.front();
        else
        {
            /// TODO: Mixed fp32 and fp64 blocks here, though coerceArgument can widen fp32 to fp64. The picker could say fp64.
            for (const auto & s : non_constant_shorts)
            {
                if (substraitSignedIntRank(s) == 0)
                    return blocked(fmt::format("arguments of {} have incompatible types ({})", name, fmt::join(non_constant_shorts, ", ")));
                if (coercion_target.empty() || substraitSignedIntRank(s) > substraitSignedIntRank(coercion_target))
                    coercion_target = s;
            }
        }
    }
    if (!coercion_target.empty())
        for (auto & arg : args)
            if (auto coerced = coerceArgument(arg, coercion_target); !coerced)
                return std::unexpected(std::move(coerced.error()));

    /// Select the overload and compose the function signature.
    std::vector<String> arg_shorts;
    for (const auto & arg : args)
        arg_shorts.push_back(substraitTypeShortName(arg.type, extensions));

    const auto accepts = [](std::string_view declared, std::string_view actual) { return declared == "any" || declared == actual; };
    const auto matches = [&](const FunctionOverload & candidate)
    {
        if (mapping.variadic)
            return std::ranges::all_of(arg_shorts, [&](const auto & s) { return accepts(candidate.arg_types.front(), s); });
        return std::ranges::equal(candidate.arg_types, arg_shorts, accepts);
    };

    const auto overload_it = std::ranges::find_if(mapping.overloads, matches);
    if (overload_it == mapping.overloads.end())
        return blocked(fmt::format("function {} has no Substrait overload for ({})", name, fmt::join(arg_shorts, ", ")));
    const FunctionOverload & overload = *overload_it;
    const std::string_view declared_return = overload.return_type;
    const String signature = fmt::format("{}:{}", mapping.substrait_name, fmt::join(overload.arg_types, "_"));

    /// A Substrait result is nullable when any of its arguments is nullable in Substrait.
    /// ClickHouse can disagree. `sum(x)` under `GROUP BY` is nullable in Substrait but not in ClickHouse.
    const bool any_arg_nullable = std::ranges::any_of(args, [](const auto & a) { return isSubstraitTypeNullable(a.type); });
    const bool result_nullable = any_arg_nullable && !mapping.never_null;
    if (isNullableOrLowCardinalityNullable(node.result_type) && !result_nullable)
        return blocked(fmt::format("function {} returns a nullable result in ClickHouse but not in Substrait", name));

    bool needs_result_cast = false;
    ConvertedExpression out;
    out.node = &node;

    /// The known exact mismatches. Substrait declares `boolean` where ClickHouse returns UInt8, with the same values and bytes.
    /// It declares `i64` where ClickHouse returns UInt64 for counts and lengths, too small to overflow `i64`.
    const DataTypePtr ch_result = removeLowCardinalityAndNullable(node.result_type);
    if ((declared_return == "bool" && isUInt8(ch_result)) || (declared_return == "i64" && isUInt64(ch_result)))
        out.type = makeSimpleSubstraitType(declared_return, result_nullable);
    else
    {
        /// Otherwise the declared return type must be the ClickHouse result type.
        if (auto converted_short = result_short(); !converted_short)
            return std::unexpected(std::move(converted_short.error()));
        out.type = std::move(*ch_result_type);
        setSubstraitTypeNullability(out.type, result_nullable);
        if (ch_result_short != declared_return)
        {
            if (mapping.coercion == Coercion::ToResultType && declared_return == coercion_target)
                needs_result_cast = true;
            else
                return blocked(
                    fmt::format("function {} returns {} in ClickHouse but {} in Substrait", name, ch_result_short, declared_return));
        }
    }

    if (needs_result_cast)
    {
        /// The computation ran widened. The cast back to the ClickHouse result type.
        const auto widened_type = makeSimpleSubstraitType(coercion_target, isSubstraitTypeNullable(out.type));
        out.expression
            = makeCast(out.type, makeScalarCall(overload.urn, signature, std::move(args), widened_type, extensions, overload.options));
    }
    else
        out.expression = makeScalarCall(overload.urn, signature, std::move(args), out.type, extensions, overload.options);

    /// `notLike` becomes `not` over `like`.
    if (mapping.negate_result)
    {
        chassert(out.type.kind_case() == substrait::Type::kBool);
        std::vector<ConvertedExpression> operand(1);
        operand.front().expression = std::move(out.expression);
        out.expression = makeScalarCall(SubstraitExtensionURN::boolean, "not:bool", std::move(operand), out.type, extensions);
    }

    return out;
}

substrait::Expression
makeConjunction(std::vector<SubstraitExpressionConverter::ConvertedExpression> conjuncts, SubstraitPlanExtensions & extensions)
{
    chassert(!conjuncts.empty());
    if (conjuncts.size() == 1)
        return std::move(conjuncts.front().expression);

    const bool any_nullable = std::ranges::any_of(conjuncts, [](const auto & c) { return isSubstraitTypeNullable(c.type); });
    const auto type = makeSimpleSubstraitType("bool", any_nullable);
    return makeScalarCall(SubstraitExtensionURN::boolean, "and:bool", std::move(conjuncts), type, extensions);
}

}

#endif
