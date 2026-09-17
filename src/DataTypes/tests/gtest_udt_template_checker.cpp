#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Common/Exception.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/tests/gtest_global_register.h>

#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseFieldFromCastedLiteral.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_MANY_BYTES;
}

namespace DB::UDT
{
namespace
{

UUID testUUID(UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = 0x550e8400e29b41d4ULL;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

DefinitionIdentity testIdentity(UInt64 low)
{
    return {
        .database_uuid = testUUID(0xa716446655440000ULL),
        .type_uuid = testUUID(low),
        .revision = 1,
    };
}

bool isRegisteredBuiltInFamilyForTest(std::string_view name) noexcept
{
    return static_cast<bool>(BuiltInDataTypeFamilyClassifier::classifyGeneric(name));
}

bool collidesWithRegisteredBuiltInFamilyOrAliasForTest(std::string_view name) noexcept
{
    return BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(name);
}

TemplateNode node(TemplateNodeKind kind)
{
    TemplateNode result;
    result.kind = kind;
    return result;
}

TemplateNode builtIn(String name, std::vector<TemplateNodeChild> children = {})
{
    auto result = node(TemplateNodeKind::BuiltIn);
    result.atom = std::move(name);
    result.children = std::move(children);
    return result;
}

TemplateNode typeFormal(UInt16 parameter)
{
    auto result = node(TemplateNodeKind::TypeParameter);
    result.parameter = parameter;
    return result;
}

ASTPtr parseType(const String & text)
{
    ParserDataType parser;
    return parseQuery(parser, text, "UDT template parser-surface inventory", 0, 150, 0);
}

const ASTExpressionList & dataTypeArguments(const ASTPtr & ast)
{
    const auto * data_type = ast->as<ASTDataType>();
    if (!data_type || !data_type->getArguments())
        throw std::logic_error("expected a generic data type with arguments");
    const auto * arguments = data_type->getArguments()->as<ASTExpressionList>();
    if (!arguments)
        throw std::logic_error("expected an ASTExpressionList data-type argument container");
    return *arguments;
}

struct AdmittedAggregateFunction
{
    TemplateNode node;
    Array parameters;
};

AdmittedAggregateFunction admitAggregateFunctionAST(const ASTPtr & ast)
{
    AdmittedAggregateFunction result;
    result.node = node(TemplateNodeKind::AggregateFunction);
    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        result.node.text = identifier->name();
        return result;
    }

    const auto * function = ast->as<ASTFunction>();
    if (!function || function->parameters)
        throw std::logic_error("unexpected aggregate-function AST inventory row");
    result.node.text = function->name;
    switch (function->getNullsAction())
    {
        case NullsAction::EMPTY: result.node.aggregate_nulls_action = AggregateFunctionNullsAction::Empty; break;
        case NullsAction::RESPECT_NULLS: result.node.aggregate_nulls_action = AggregateFunctionNullsAction::RespectNulls; break;
        case NullsAction::IGNORE_NULLS: result.node.aggregate_nulls_action = AggregateFunctionNullsAction::IgnoreNulls; break;
    }
    if (function->arguments)
    {
        const auto * arguments = function->arguments->as<ASTExpressionList>();
        if (!arguments)
            throw std::logic_error("aggregate-function parameters are not an ASTExpressionList");
        result.parameters.reserve(arguments->children.size());
        for (const auto & parameter : arguments->children)
            result.parameters.push_back(parseFieldFromCastedLiteral(parameter));
    }
    return result;
}

DefinitionInput scalarDefinition(String name, UInt64 identity_low, String family = "UInt64")
{
    DefinitionInput result;
    result.identity = testIdentity(identity_low);
    result.normalized_name = std::move(name);
    result.nodes.push_back(builtIn(std::move(family)));
    return result;
}

DefinitionInput aliasDefinition(String name, UInt64 identity_low)
{
    DefinitionInput result;
    result.identity = testIdentity(identity_low);
    result.normalized_name = std::move(name);
    result.parameters.push_back({.normalized_name = "T", .kind = ParameterKind::Type});
    result.nodes.push_back(typeFormal(0));
    return result;
}

DefinitionInput callDefinition(
    String name,
    UInt64 identity_low,
    UInt64 target_identity_low,
    ParameterKind caller_parameter_kind = ParameterKind::Type,
    bool include_actual = true)
{
    DefinitionInput result;
    result.identity = testIdentity(identity_low);
    result.normalized_name = std::move(name);
    result.parameters.push_back({.normalized_name = "T", .kind = caller_parameter_kind});
    auto call = node(TemplateNodeKind::DefinitionCall);
    if (include_actual)
        call.children.push_back({.reference = 0, .label = {}});
    result.nodes.push_back(std::move(call));
    result.dependencies.push_back({.type_uuid = testUUID(target_identity_low), .revision = 1, .target_definition_hash = {}});
    return result;
}

DefinitionInput recursiveDefinition(String name, UInt64 identity_low)
{
    DefinitionInput result;
    result.identity = testIdentity(identity_low);
    result.normalized_name = std::move(name);
    result.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt64},
    };
    result.decreasing_parameter = 1;
    result.checker_abi = 2;

    auto type_if = node(TemplateNodeKind::TypeIfZero);
    type_if.parameter = 1;
    type_if.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    auto array = builtIn("Array", {{.reference = 3, .label = {}}});
    auto self = node(TemplateNodeKind::SelfCall);
    self.parameter = 1;
    self.decrement = 1;
    result.nodes = {std::move(type_if), typeFormal(0), std::move(array), std::move(self)};
    return result;
}

DefinitionInput representativeCanonicalIRDefinition(char binary_suffix)
{
    DefinitionInput result;
    result.identity = testIdentity(0x100);
    result.normalized_name = "Representative";
    result.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt64},
    };

    auto root = builtIn("Tuple");
    for (TemplateNodeID reference = 1; reference <= 9; ++reference)
        root.children.push_back({.reference = reference, .label = {}});

    auto signed_literal = node(TemplateNodeKind::SignedLiteral);
    signed_literal.signed_literal = -42;
    auto string_literal = node(TemplateNodeKind::StringLiteral);
    const std::array<char, 3> binary{'A', '\0', binary_suffix};
    string_literal.text.assign(binary.data(), binary.size());
    auto identifier = node(TemplateNodeKind::Identifier);
    identifier.text = "timezone";
    auto specialized_enum = node(TemplateNodeKind::SpecializedEnum);
    specialized_enum.specialized_enum_width = SpecializedEnumWidth::Enum8;
    specialized_enum.enum_entries = {{.name = "", .value = -128}, {.name = "ready", .value = 127}};
    auto unsigned_literal = node(TemplateNodeKind::UnsignedLiteral);
    unsigned_literal.unsigned_literal = 300;
    auto boolean_literal = node(TemplateNodeKind::BooleanLiteral);
    boolean_literal.boolean_literal = true;
    auto value_formal = node(TemplateNodeKind::ValueParameter);
    value_formal.parameter = 1;
    auto type_if = node(TemplateNodeKind::TypeIfZero);
    type_if.parameter = 1;
    type_if.children = {{.reference = 10, .label = {}}, {.reference = 11, .label = {}}};

    result.nodes = {
        std::move(root),
        std::move(signed_literal),
        std::move(string_literal),
        std::move(identifier),
        std::move(specialized_enum),
        std::move(unsigned_literal),
        std::move(boolean_literal),
        typeFormal(0),
        std::move(value_formal),
        std::move(type_if),
        builtIn("UInt64"),
        builtIn("String"),
    };
    return result;
}

template <typename Function>
void expectDBError(int expected_code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected DB::Exception";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), expected_code) << error.message();
    }
}

template <typename Function>
void expectExactDBError(int expected_code, std::string_view expected_message, Function && function)
{
    try
    {
        function();
        FAIL() << "expected DB::Exception";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), expected_code) << error.message();
        EXPECT_EQ(error.message(), expected_message);
    }
}

const Definition & byName(const std::vector<Definition::Ptr> & definitions, std::string_view name)
{
    const auto found = std::find_if(
        definitions.begin(), definitions.end(), [&](const auto & definition) { return definition->getNormalizedName() == name; });
    if (found == definitions.end())
        throw std::logic_error("checked definition is absent");
    return **found;
}

std::span<const CheckerProof::Byte> asProofBytes(const String & value)
{
    return {reinterpret_cast<const CheckerProof::Byte *>(value.data()), value.size()};
}

DefinitionInput aggregateArrayParameterDefinition(const Array & parameters)
{
    DefinitionInput result;
    result.identity = testIdentity(0x102);
    result.normalized_name = "AggregateArrayParameter";
    result.nodes = {
        builtIn("AggregateFunction"),
        node(TemplateNodeKind::UnsignedLiteral),
        node(TemplateNodeKind::AggregateFunction),
        builtIn("Array"),
        builtIn("UInt64"),
    };
    result.nodes[1].unsigned_literal = 1;
    result.nodes[2].text = "sumMapFiltered";
    result.nodes[0].children = {
        {.reference = 1, .label = {}},
        {.reference = 2, .label = {}},
        {.reference = 3, .label = {}},
        {.reference = 3, .label = {}},
    };
    result.nodes[3].children = {{.reference = 4, .label = {}}};
    const auto parameter_roots = appendCanonicalFieldValues(parameters, result.nodes);
    for (const auto reference : parameter_roots)
        result.nodes[2].children.push_back({.reference = reference, .label = {}});
    return result;
}

DefinitionInput completeTypedSurfaceDefinition()
{
    DefinitionInput result;
    result.identity = testIdentity(0x103);
    result.normalized_name = "CompleteTypedSurface";
    result.nodes = {
        builtIn("Tuple"),
        builtIn("AggregateFunction"),
        builtIn("Dynamic"),
        builtIn("JSON"),
        node(TemplateNodeKind::UnsignedLiteral),
        node(TemplateNodeKind::AggregateFunction),
        builtIn("Array"),
        node(TemplateNodeKind::DynamicSetting),
        node(TemplateNodeKind::ObjectSetting),
        node(TemplateNodeKind::ObjectSetting),
        node(TemplateNodeKind::ObjectTypedPath),
        node(TemplateNodeKind::ObjectSkipPath),
        node(TemplateNodeKind::ObjectSkipRegexp),
        builtIn("UInt64"),
    };
    result.nodes[0].children = {
        {.reference = 1, .label = {}},
        {.reference = 2, .label = {}},
        {.reference = 3, .label = {}},
    };
    result.nodes[1].children = {
        {.reference = 4, .label = {}},
        {.reference = 5, .label = {}},
        {.reference = 6, .label = {}},
        {.reference = 6, .label = {}},
    };
    result.nodes[4].unsigned_literal = 1;
    result.nodes[5].text = "sumMapFiltered";
    result.nodes[6].children = {{.reference = 13, .label = {}}};
    result.nodes[7].text = "max_types";
    result.nodes[7].children = {{.reference = 14, .label = {}}};
    result.nodes[2].children = {{.reference = 7, .label = {}}};
    result.nodes[8].text = "max_dynamic_types";
    result.nodes[8].children = {{.reference = 15, .label = {}}};
    result.nodes[9].text = "max_dynamic_paths";
    result.nodes[9].children = {{.reference = 16, .label = {}}};
    result.nodes[10].text = "payload.value";
    result.nodes[10].children = {{.reference = 13, .label = {}}};
    result.nodes[11].text = "private.path";
    result.nodes[12].text.assign("^tmp\0$", 6);
    result.nodes[3].children = {
        {.reference = 8, .label = {}},
        {.reference = 9, .label = {}},
        {.reference = 10, .label = {}},
        {.reference = 11, .label = {}},
        {.reference = 12, .label = {}},
    };
    for (UInt64 value : {7, 9, 11})
    {
        auto literal = node(TemplateNodeKind::UnsignedLiteral);
        literal.unsigned_literal = value;
        result.nodes.push_back(std::move(literal));
    }
    Array parameters{Field(Array{Field(UInt64{1}), Field(UInt64{4}), Field(UInt64{8})})};
    const auto parameter_roots = appendCanonicalFieldValues(parameters, result.nodes);
    result.nodes[5].children = {{.reference = parameter_roots.front(), .label = {}}};
    return result;
}

TEST(UDTTemplateChecker, ParserFactoryAndCanonicalIRInventoryCoversTypedDataTypeSurfaces)
{
    const auto plain = parseType("AggregateFunction(sum, UInt64)");
    const auto empty_call = parseType("AggregateFunction(sum(), UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(plain)));
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(empty_call)));
    const auto & plain_arguments = dataTypeArguments(plain);
    const auto & empty_call_arguments = dataTypeArguments(empty_call);
    ASSERT_GE(plain_arguments.children.size(), 2);
    ASSERT_GE(empty_call_arguments.children.size(), 2);
    ASSERT_NE(plain_arguments.children[0]->as<ASTIdentifier>(), nullptr);
    ASSERT_NE(empty_call_arguments.children[0]->as<ASTFunction>(), nullptr);
    EXPECT_EQ(
        admitAggregateFunctionAST(plain_arguments.children[0]).node, admitAggregateFunctionAST(empty_call_arguments.children[0]).node);

    const auto array_parameter_type = parseType("AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))");
    const auto factory_type = DataTypeFactory::instance().get(array_parameter_type);
    EXPECT_EQ(factory_type->getTypeId(), TypeIndex::AggregateFunction);
    const auto & array_arguments = dataTypeArguments(array_parameter_type);
    ASSERT_EQ(array_arguments.children.size(), 4);
    const auto * version = array_arguments.children[0]->as<ASTLiteral>();
    ASSERT_NE(version, nullptr);
    EXPECT_EQ(version->value.safeGet<UInt64>(), 1);
    auto aggregate = admitAggregateFunctionAST(array_arguments.children[1]);
    ASSERT_EQ(aggregate.parameters.size(), 1);
    ASSERT_EQ(aggregate.parameters.front().getType(), Field::Types::Array);
    EXPECT_EQ(aggregate.parameters.front().safeGet<Array>().size(), 3);

    const auto checked = TemplateChecker::checkAll({aggregateArrayParameterDefinition(aggregate.parameters)});
    ASSERT_EQ(checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(checked.front()->getCertificate().canonical_template_ir)));

    const auto respect_nulls = parseType("AggregateFunction(any() RESPECT NULLS, UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(respect_nulls)));
    const auto admitted_respect = admitAggregateFunctionAST(dataTypeArguments(respect_nulls).children[0]);
    EXPECT_EQ(admitted_respect.node.aggregate_nulls_action, AggregateFunctionNullsAction::RespectNulls);

    const auto ignore_nulls = parseType("AggregateFunction(any() IGNORE NULLS, UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(ignore_nulls)));
    const auto admitted_ignore = admitAggregateFunctionAST(dataTypeArguments(ignore_nulls).children[0]);
    EXPECT_EQ(admitted_ignore.node.aggregate_nulls_action, AggregateFunctionNullsAction::IgnoreNulls);

    /// The current SimpleAggregateFunction creator accepts this parser shape
    /// but ignores its NULL action. Canonical template admission rejects that
    /// invisible state below instead of hashing two physically equal types.
    const auto simple_ignored_nulls = parseType("SimpleAggregateFunction(sum() RESPECT NULLS, UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(simple_ignored_nulls)));
    EXPECT_EQ(
        admitAggregateFunctionAST(dataTypeArguments(simple_ignored_nulls).children[0]).node.aggregate_nulls_action,
        AggregateFunctionNullsAction::RespectNulls);

    const auto direct_float = parseType("AggregateFunction(quantiles(-0.0), UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(direct_float)));
    const auto admitted_float = admitAggregateFunctionAST(dataTypeArguments(direct_float).children[0]);
    ASSERT_EQ(admitted_float.parameters.size(), 1);
    EXPECT_EQ(admitted_float.parameters.front().getType(), Field::Types::Float64);
    EXPECT_TRUE(std::signbit(admitted_float.parameters.front().safeGet<Float64>()));

    /// Null is a ParserDataType/parseFieldFromCastedLiteral shape even though
    /// whether a particular aggregate function accepts it is registry-specific.
    const auto direct_null = parseType("AggregateFunction(inventory(NULL), UInt64)");
    const auto admitted_null = admitAggregateFunctionAST(dataTypeArguments(direct_null).children[0]);
    ASSERT_EQ(admitted_null.parameters.size(), 1);
    EXPECT_EQ(admitted_null.parameters.front().getType(), Field::Types::Null);

    const auto casted = parseType("AggregateFunction(quantiles(CAST('0.5', 'Float64')), UInt64)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(casted)));
    const auto admitted_casted = admitAggregateFunctionAST(dataTypeArguments(casted).children[0]);
    ASSERT_EQ(admitted_casted.parameters.size(), 1);
    EXPECT_EQ(admitted_casted.parameters.front().getType(), Field::Types::Float64);
    EXPECT_EQ(admitted_casted.parameters.front().safeGet<Float64>(), 0.5);

    const std::array cast_inventory{
        std::pair{"CAST('1.25', 'Decimal32(2)')", Field::Types::Decimal32},
        std::pair{"CAST('1.25', 'Decimal64(2)')", Field::Types::Decimal64},
        std::pair{"CAST('1.25', 'Decimal128(2)')", Field::Types::Decimal128},
        std::pair{"CAST('1.25', 'Decimal256(2)')", Field::Types::Decimal256},
        std::pair{"CAST('true', 'Bool')", Field::Types::Bool},
        std::pair{"CAST('-0', 'Float64')", Field::Types::Float64},
        std::pair{"CAST('binary-safe', 'String')", Field::Types::String},
        std::pair{"CAST('-42', 'Int64')", Field::Types::Int64},
        std::pair{"CAST('42', 'UInt64')", Field::Types::UInt64},
        std::pair{"CAST('-42', 'Int128')", Field::Types::Int128},
        std::pair{"CAST('42', 'UInt128')", Field::Types::UInt128},
        std::pair{"CAST('-42', 'Int256')", Field::Types::Int256},
        std::pair{"CAST('42', 'UInt256')", Field::Types::UInt256},
        std::pair{"CAST('00000000-0000-0000-0000-00000000002a', 'UUID')", Field::Types::UUID},
        std::pair{"CAST('192.0.2.42', 'IPv4')", Field::Types::IPv4},
        std::pair{"CAST('2001:db8::2a', 'IPv6')", Field::Types::IPv6},
    };
    Array cast_fields;
    cast_fields.reserve(cast_inventory.size());
    for (const auto & [expression, expected_type] : cast_inventory)
    {
        const auto inventory_type = parseType("AggregateFunction(inventory(" + String(expression) + "), UInt64)");
        auto admitted = admitAggregateFunctionAST(dataTypeArguments(inventory_type).children[0]);
        ASSERT_EQ(admitted.parameters.size(), 1);
        EXPECT_EQ(admitted.parameters.front().getType(), expected_type) << expression;
        cast_fields.push_back(std::move(admitted.parameters.front()));
    }

    DefinitionInput cast_definition;
    cast_definition.identity = testIdentity(0x108);
    cast_definition.normalized_name = "CastedFieldInventory";
    cast_definition.nodes = {builtIn("AggregateFunction"), node(TemplateNodeKind::AggregateFunction), builtIn("UInt64")};
    cast_definition.nodes[0].children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    cast_definition.nodes[1].text = "inventory";
    for (const auto field_root : appendCanonicalFieldValues(cast_fields, cast_definition.nodes))
        cast_definition.nodes[1].children.push_back({.reference = field_root, .label = {}});
    const auto cast_checked = TemplateChecker::checkAll({std::move(cast_definition)});
    ASSERT_EQ(cast_checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(cast_checked.front()->getCertificate().canonical_template_ir)));

    const auto dynamic = parseType("Dynamic(max_types=7)");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(dynamic)));
    const auto & dynamic_arguments = dataTypeArguments(dynamic);
    ASSERT_EQ(dynamic_arguments.children.size(), 1);
    const auto * dynamic_setting = dynamic_arguments.children[0]->as<ASTFunction>();
    ASSERT_NE(dynamic_setting, nullptr);
    EXPECT_EQ(dynamic_setting->name, "equals");
    ASSERT_NE(dynamic_setting->arguments, nullptr);
    ASSERT_EQ(dynamic_setting->arguments->children.size(), 2);
    EXPECT_EQ(dynamic_setting->arguments->children[0]->as<ASTIdentifier>()->name(), "max_types");
    EXPECT_EQ(dynamic_setting->arguments->children[1]->as<ASTLiteral>()->value.safeGet<UInt64>(), 7);

    const auto object
        = parseType("JSON(max_dynamic_types=7, max_dynamic_paths=9, payload.value UInt64, SKIP private.path, SKIP REGEXP '^tmp')");
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(object)));
    const auto & object_arguments = dataTypeArguments(object);
    ASSERT_EQ(object_arguments.children.size(), 5);
    std::array<size_t, 4> object_variant_counts{};
    bool saw_max_dynamic_types = false;
    bool saw_max_dynamic_paths = false;
    bool saw_typed_path = false;
    bool saw_skip_path = false;
    bool saw_skip_regexp = false;
    for (const auto & argument : object_arguments.children)
    {
        const auto * object_argument = argument->as<ASTObjectTypeArgument>();
        ASSERT_NE(object_argument, nullptr);
        object_variant_counts[0] += object_argument->parameter != nullptr;
        object_variant_counts[1] += object_argument->path_with_type != nullptr;
        object_variant_counts[2] += object_argument->skip_path != nullptr;
        object_variant_counts[3] += object_argument->skip_path_regexp != nullptr;
        if (object_argument->parameter)
        {
            const auto * setting = object_argument->parameter->as<ASTFunction>();
            ASSERT_NE(setting, nullptr);
            ASSERT_NE(setting->arguments, nullptr);
            ASSERT_EQ(setting->arguments->children.size(), 2);
            const auto * setting_name = setting->arguments->children[0]->as<ASTIdentifier>();
            const auto * setting_value = setting->arguments->children[1]->as<ASTLiteral>();
            ASSERT_NE(setting_name, nullptr);
            ASSERT_NE(setting_value, nullptr);
            if (setting_name->name() == "max_dynamic_types")
            {
                saw_max_dynamic_types = true;
                EXPECT_EQ(setting_value->value.safeGet<UInt64>(), 7);
            }
            else if (setting_name->name() == "max_dynamic_paths")
            {
                saw_max_dynamic_paths = true;
                EXPECT_EQ(setting_value->value.safeGet<UInt64>(), 9);
            }
            else
                ADD_FAILURE() << "unexpected JSON setting " << setting_name->name();
        }
        else if (object_argument->path_with_type)
        {
            const auto * typed_path = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            ASSERT_NE(typed_path, nullptr);
            saw_typed_path = true;
            EXPECT_EQ(typed_path->path, "payload.value");
            ASSERT_NE(typed_path->type, nullptr);
            const auto * typed_path_type = typed_path->type->as<ASTDataType>();
            ASSERT_NE(typed_path_type, nullptr);
            EXPECT_EQ(typed_path_type->name, "UInt64");
        }
        else if (object_argument->skip_path)
        {
            const auto * skipped = object_argument->skip_path->as<ASTIdentifier>();
            ASSERT_NE(skipped, nullptr);
            saw_skip_path = true;
            EXPECT_EQ(skipped->name(), "private.path");
        }
        else if (object_argument->skip_path_regexp)
        {
            const auto * skipped = object_argument->skip_path_regexp->as<ASTLiteral>();
            ASSERT_NE(skipped, nullptr);
            saw_skip_regexp = true;
            EXPECT_EQ(skipped->value.safeGet<String>(), "^tmp");
        }
    }
    EXPECT_EQ(object_variant_counts, (std::array<size_t, 4>{2, 1, 1, 1}));
    EXPECT_TRUE(saw_max_dynamic_types);
    EXPECT_TRUE(saw_max_dynamic_paths);
    EXPECT_TRUE(saw_typed_path);
    EXPECT_TRUE(saw_skip_path);
    EXPECT_TRUE(saw_skip_regexp);

    const auto named_tuple = parseType("Tuple(id UInt64, payload String)");
    const auto specialized_enum = parseType("Enum('large' = 300, 'small' = -2)");
    const auto * tuple_ast = named_tuple->as<ASTTupleDataType>();
    const auto * enum_ast = specialized_enum->as<ASTEnumDataType>();
    ASSERT_NE(tuple_ast, nullptr);
    ASSERT_NE(enum_ast, nullptr);
    EXPECT_NO_THROW(static_cast<void>(DataTypeFactory::instance().get(named_tuple)));
    const auto enum_type = DataTypeFactory::instance().get(specialized_enum);
    EXPECT_EQ(enum_type->getTypeId(), TypeIndex::Enum16);

    const auto * tuple_arguments = tuple_ast->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(tuple_arguments, nullptr);
    ASSERT_EQ(tuple_ast->element_names, (Strings{"id", "payload"}));
    ASSERT_EQ(tuple_arguments->children.size(), tuple_ast->element_names.size());
    DefinitionInput tuple_definition;
    tuple_definition.identity = testIdentity(0x109);
    tuple_definition.normalized_name = "SpecializedNamedTuple";
    tuple_definition.nodes = {builtIn("Tuple")};
    for (size_t index = 0; index < tuple_arguments->children.size(); ++index)
    {
        const auto * element_type = tuple_arguments->children[index]->as<ASTDataType>();
        ASSERT_NE(element_type, nullptr);
        tuple_definition.nodes[0].children.push_back(
            {.reference = static_cast<TemplateNodeID>(tuple_definition.nodes.size()), .label = tuple_ast->element_names[index]});
        tuple_definition.nodes.push_back(builtIn(element_type->name));
    }
    const auto tuple_checked = TemplateChecker::checkAll({std::move(tuple_definition)});
    ASSERT_EQ(tuple_checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(tuple_checked.front()->getCertificate().canonical_template_ir)));

    auto enum_entries = enum_ast->values;
    std::sort(enum_entries.begin(), enum_entries.end(), [](const auto & lhs, const auto & rhs) { return lhs.second < rhs.second; });
    DefinitionInput enum_definition;
    enum_definition.identity = testIdentity(0x10a);
    enum_definition.normalized_name = "SpecializedCanonicalEnum";
    auto enum_node = node(TemplateNodeKind::SpecializedEnum);
    enum_node.specialized_enum_width = SpecializedEnumWidth::Enum16;
    for (auto & [name, value] : enum_entries)
        enum_node.enum_entries.push_back({.name = std::move(name), .value = value});
    enum_definition.nodes.push_back(std::move(enum_node));
    const auto enum_checked = TemplateChecker::checkAll({std::move(enum_definition)});
    ASSERT_EQ(enum_checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(enum_checked.front()->getCertificate().canonical_template_ir)));
}

TEST(UDTTemplateChecker, CanonicalFieldGraphPreservesEveryBinaryFieldKindAndBoundsConstruction)
{
    const Float64 negative_zero = std::bit_cast<Float64>(UInt64{1} << 63);
    const Float64 payload_nan = std::bit_cast<Float64>(UInt64{0x7ff8000000000042ULL});
    const Float64 other_payload_nan = std::bit_cast<Float64>(UInt64{0x7ff8000000000043ULL});
    EXPECT_NE(CanonicalFieldValue::fromField(Field(negative_zero)), CanonicalFieldValue::fromField(Field(Float64{0})));
    EXPECT_NE(CanonicalFieldValue::fromField(Field(payload_nan)), CanonicalFieldValue::fromField(Field(other_payload_nan)));
    EXPECT_EQ(CanonicalFieldValue::fromField(Field(POSITIVE_INFINITY)).kind, CanonicalFieldKind::PositiveInfinity);
    EXPECT_EQ(CanonicalFieldValue::fromField(Field(NEGATIVE_INFINITY)).kind, CanonicalFieldKind::NegativeInfinity);
    EXPECT_EQ(CanonicalFieldValue::fromField(Field(String{"a\0b", 3})).payload, String("a\0b", 3));
    const Field invalid_null(Null{static_cast<Null::Value>(2)});
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(CanonicalFieldValue::fromField(invalid_null)); });

    Object object;
    object.emplace("a", Field(UInt64{1}));
    object.emplace("b", Field(String("two")));
    Map map{
        Field(Tuple{Field(UInt64{3}), Field(String("three"))}),
        Field(Tuple{Field(UInt64{4}), Field(Object(object))}),
    };
    Array roots{
        Field(Array{Field(UInt64{1}), Field(Null{})}),
        Field(Tuple{Field(true), Field(DecimalField<Decimal64>(42, 3))}),
        Field(map),
        Field(object),
        Field(AggregateFunctionStateData{.name = "AggregateFunction(sum, UInt64)", .data = String{"x\0y", 3}}),
    };
    std::vector<TemplateNode> nodes;
    const auto root_ids = appendCanonicalFieldValues(roots, nodes);
    ASSERT_EQ(root_ids.size(), roots.size());
    ASSERT_GT(nodes.size(), roots.size());
    EXPECT_EQ(nodes[root_ids[0]].field_value.kind, CanonicalFieldKind::Array);
    EXPECT_EQ(nodes[root_ids[1]].field_value.kind, CanonicalFieldKind::Tuple);
    EXPECT_EQ(nodes[root_ids[2]].field_value.kind, CanonicalFieldKind::Map);
    EXPECT_EQ(nodes[root_ids[3]].field_value.kind, CanonicalFieldKind::Object);
    EXPECT_EQ(nodes[root_ids[4]].field_value.kind, CanonicalFieldKind::AggregateFunctionState);
    ASSERT_EQ(nodes[root_ids[3]].children.size(), 2);
    EXPECT_EQ(nodes[root_ids[3]].children[0].label, "a");
    EXPECT_EQ(nodes[root_ids[3]].children[1].label, "b");

    const auto before = nodes;
    CanonicalFieldValueLimits limits;
    limits.maximum_depth = 1;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { appendCanonicalFieldValues(std::span<const Field>(roots).first(1), nodes, limits); });
    EXPECT_EQ(nodes, before);

    Array large_string{Field(String(8ULL << 10, 's'))};
    limits = {};
    limits.maximum_literal_bytes = large_string.front().safeGet<String>().size() - 1;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { appendCanonicalFieldValues(large_string, nodes, limits); });
    EXPECT_EQ(nodes, before);

    Array large_state{Field(AggregateFunctionStateData{.name = String(4ULL << 10, 'n'), .data = String(4ULL << 10, 'd')})};
    const auto & state = large_state.front().safeGet<AggregateFunctionStateData>();
    limits = {};
    limits.maximum_literal_bytes = state.name.size() + state.data.size() - 1;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { appendCanonicalFieldValues(large_state, nodes, limits); });
    EXPECT_EQ(nodes, before);

    Object large_key_object;
    large_key_object.emplace(String(8ULL << 10, 'k'), Field(UInt64{1}));
    Array large_object{Field(std::move(large_key_object))};
    limits = {};
    limits.maximum_literal_bytes = large_object.front().safeGet<Object>().begin()->first.size() - 1;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { appendCanonicalFieldValues(large_object, nodes, limits); });
    EXPECT_EQ(nodes, before);

    Field deep = Field(UInt64{1});
    for (size_t level = 0; level < 4; ++level)
        deep = Field(Array{std::move(deep)});
    Array deep_root{std::move(deep)};
    limits = {};
    limits.maximum_nodes = 4;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { appendCanonicalFieldValues(deep_root, nodes, limits); });
    EXPECT_EQ(nodes, before);

    Array malformed_map{Field(Map{Field(UInt64{1})})};
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { appendCanonicalFieldValues(malformed_map, nodes); });
    EXPECT_EQ(nodes, before);

    Array invalid_null_root{invalid_null};
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { appendCanonicalFieldValues(invalid_null_root, nodes); });
    EXPECT_EQ(nodes, before);
}

TEST(UDTTemplateChecker, CanonicalFieldLeafInventoryRoundTripsTheClosedFieldBinarySurface)
{
    struct InventoryRow
    {
        Field value;
        CanonicalFieldKind canonical_kind;
    };

    /// The parser/factory inventory test above covers the SQL-reachable
    /// parameter spellings, including a composite Array and every admitted
    /// CAST family. This table closes the wider FieldBinaryEncoding contract.
    /// AggregateFunctionState is intentionally present even though
    /// ParserDataType cannot spell it; CustomType is intentionally absent
    /// because FieldBinaryEncoding itself rejects it as unsupported.
    const std::array inventory{
        InventoryRow{Field(Null{}), CanonicalFieldKind::Null},
        InventoryRow{Field(NEGATIVE_INFINITY), CanonicalFieldKind::NegativeInfinity},
        InventoryRow{Field(POSITIVE_INFINITY), CanonicalFieldKind::PositiveInfinity},
        InventoryRow{Field(UInt64{42}), CanonicalFieldKind::UInt64},
        InventoryRow{Field(Int64{-42}), CanonicalFieldKind::Int64},
        InventoryRow{Field(UInt128{42}), CanonicalFieldKind::UInt128},
        InventoryRow{Field(Int128{-42}), CanonicalFieldKind::Int128},
        InventoryRow{Field(UInt256{42}), CanonicalFieldKind::UInt256},
        InventoryRow{Field(Int256{-42}), CanonicalFieldKind::Int256},
        InventoryRow{Field(Float64{-0.0}), CanonicalFieldKind::Float64},
        InventoryRow{Field(String{"a\0b", 3}), CanonicalFieldKind::String},
        InventoryRow{Field(true), CanonicalFieldKind::Bool},
        InventoryRow{Field(DecimalField<Decimal32>(42, 3)), CanonicalFieldKind::Decimal32},
        InventoryRow{Field(DecimalField<Decimal64>(42, 3)), CanonicalFieldKind::Decimal64},
        InventoryRow{Field(DecimalField<Decimal128>(Int128{42}, 3)), CanonicalFieldKind::Decimal128},
        InventoryRow{Field(DecimalField<Decimal256>(Int256{42}, 3)), CanonicalFieldKind::Decimal256},
        InventoryRow{Field(UUID{42}), CanonicalFieldKind::UUID},
        InventoryRow{Field(IPv4{42}), CanonicalFieldKind::IPv4},
        InventoryRow{Field(IPv6{42}), CanonicalFieldKind::IPv6},
        InventoryRow{Field(Array{}), CanonicalFieldKind::Array},
        InventoryRow{Field(Tuple{}), CanonicalFieldKind::Tuple},
        InventoryRow{Field(Map{}), CanonicalFieldKind::Map},
        InventoryRow{Field(Object{}), CanonicalFieldKind::Object},
        InventoryRow{Field(AggregateFunctionStateData{.name = {}, .data = String{"x\0y", 3}}), CanonicalFieldKind::AggregateFunctionState},
    };

    Array decoded_values;
    decoded_values.reserve(inventory.size());
    for (const auto & row : inventory)
    {
        WriteBufferFromOwnString encoded;
        encodeField(row.value, encoded);
        ReadBufferFromString input(encoded.str());
        const Field decoded = decodeField(input);
        ASSERT_TRUE(input.eof());
        EXPECT_EQ(decoded, row.value);

        const auto canonical = CanonicalFieldValue::fromField(decoded);
        EXPECT_EQ(canonical.kind, row.canonical_kind);
        decoded_values.push_back(decoded);
    }

    DefinitionInput definition;
    definition.identity = testIdentity(0x107);
    definition.normalized_name = "ClosedFieldBinarySurface";
    definition.nodes = {builtIn("AggregateFunction"), node(TemplateNodeKind::AggregateFunction), builtIn("UInt64")};
    definition.nodes[0].children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
    definition.nodes[1].text = "closedFieldInventory";
    const auto field_roots = appendCanonicalFieldValues(decoded_values, definition.nodes);
    for (const auto field_root : field_roots)
        definition.nodes[1].children.push_back({.reference = field_root, .label = {}});
    const auto checked = TemplateChecker::checkAll({std::move(definition)});
    ASSERT_EQ(checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(checked.front()->getCertificate().canonical_template_ir)));

    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(CanonicalFieldValue::fromField(Field(CustomType{}))); });
}

TEST(UDTTemplateChecker, CheckerAcceptsTheCompleteTypedSurfaceAndRejectsMisplacedNodesPrecisely)
{
    const auto checked = TemplateChecker::checkAll({completeTypedSurfaceDefinition()});
    ASSERT_EQ(checked.size(), 1);
    EXPECT_NO_THROW(
        CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(checked.front()->getCertificate().canonical_template_ir)));

    auto bad_setting = completeTypedSurfaceDefinition();
    bad_setting.identity = testIdentity(0x104);
    bad_setting.normalized_name = "BadSetting";
    bad_setting.nodes[7].text = "max_paths";
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(TemplateChecker::checkAll({std::move(bad_setting)})); });

    auto misplaced = scalarDefinition("Misplaced", 0x105);
    auto setting = node(TemplateNodeKind::DynamicSetting);
    setting.text = "max_types";
    setting.children = {{.reference = 2, .label = {}}};
    auto setting_value = node(TemplateNodeKind::UnsignedLiteral);
    setting_value.unsigned_literal = 7;
    misplaced.nodes = {builtIn("Array", {{.reference = 1, .label = {}}}), std::move(setting), std::move(setting_value)};
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(TemplateChecker::checkAll({std::move(misplaced)})); });

    auto odd_map = completeTypedSurfaceDefinition();
    odd_map.identity = testIdentity(0x106);
    odd_map.normalized_name = "OddMap";
    const auto field_root = odd_map.nodes[5].children.front().reference;
    odd_map.nodes[field_root].field_value.kind = CanonicalFieldKind::Map;
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(TemplateChecker::checkAll({std::move(odd_map)})); });
}

TEST(UDTTemplateChecker, RawCanonicalIRShapeNeverSubstitutesForSemanticAdmission)
{
    DefinitionInput wrong_aggregate_child;
    wrong_aggregate_child.identity = testIdentity(0x10b);
    wrong_aggregate_child.normalized_name = "WrongAggregateChild";
    wrong_aggregate_child.nodes = {
        builtIn("AggregateFunction", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        node(TemplateNodeKind::AggregateFunction),
        builtIn("UInt64"),
        node(TemplateNodeKind::UnsignedLiteral),
    };
    wrong_aggregate_child.nodes[1].text = "sum";
    wrong_aggregate_child.nodes[1].children = {{.reference = 3, .label = {}}};
    wrong_aggregate_child.nodes[3].unsigned_literal = 1;
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: aggregate-function parameter is not a Field value or value parameter",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(wrong_aggregate_child)})); });

    DefinitionInput duplicate_enum_labels;
    duplicate_enum_labels.identity = testIdentity(0x10c);
    duplicate_enum_labels.normalized_name = "DuplicateEnumLabels";
    auto duplicate_enum = node(TemplateNodeKind::SpecializedEnum);
    duplicate_enum.specialized_enum_width = SpecializedEnumWidth::Enum8;
    duplicate_enum.enum_entries = {{.name = "same", .value = 0}, {.name = "same", .value = 1}};
    duplicate_enum_labels.nodes.push_back(std::move(duplicate_enum));
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: specialized Enum labels are not unique",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(duplicate_enum_labels)})); });
}

TEST(UDTTemplateChecker, CanonicalParserSurfaceRejectsInvisibleOrNormalizedDifferences)
{
    auto labelled_array = scalarDefinition("LabelledArray", 0x10d);
    labelled_array.nodes = {
        builtIn("Array", {{.reference = 1, .label = "not-a-field"}}),
        builtIn("UInt64"),
    };
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: only Tuple/Nested type arguments may carry field labels",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(labelled_array)})); });

    auto partially_named_tuple = scalarDefinition("PartiallyNamedTuple", 0x10e);
    partially_named_tuple.nodes = {
        builtIn("Tuple", {{.reference = 1, .label = "first"}, {.reference = 2, .label = {}}}),
        builtIn("UInt64"),
        builtIn("String"),
    };
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: Tuple/Nested field labels are only partially specified",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(partially_named_tuple)})); });

    auto reordered_object = completeTypedSurfaceDefinition();
    reordered_object.identity = testIdentity(0x10f);
    reordered_object.normalized_name = "ReorderedObject";
    std::swap(reordered_object.nodes[3].children[0], reordered_object.nodes[3].children[1]);
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: JSON/Object arguments are not in canonical factory order",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(reordered_object)})); });

    DefinitionInput ignored_simple_nulls;
    ignored_simple_nulls.identity = testIdentity(0x110);
    ignored_simple_nulls.normalized_name = "IgnoredSimpleNulls";
    ignored_simple_nulls.nodes = {
        builtIn("SimpleAggregateFunction", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}}),
        node(TemplateNodeKind::AggregateFunction),
        builtIn("UInt64"),
    };
    ignored_simple_nulls.nodes[1].text = "sum";
    ignored_simple_nulls.nodes[1].aggregate_nulls_action = AggregateFunctionNullsAction::RespectNulls;
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: SimpleAggregateFunction cannot carry an ignored NULL action",
        [&] { static_cast<void>(TemplateChecker::checkAll({std::move(ignored_simple_nulls)})); });
}

TEST(UDTTemplateChecker, AcceptsCompleteCanonicalIRAndStrictDecreasingSelfRecursion)
{
    const auto representative = TemplateChecker::checkAll({representativeCanonicalIRDefinition('B')});
    ASSERT_EQ(representative.size(), 1);
    const auto & rich = *representative.front();
    EXPECT_NO_THROW(CheckerProof::validateEncodedCanonicalTemplateIR(asProofBytes(rich.getCertificate().canonical_template_ir)));
    EXPECT_NE(rich.getCertificate().canonical_template_ir.find(std::string_view{"A\0B", 3}), String::npos);
    EXPECT_EQ(rich.getCertificate().logical_node_count, 12);
    EXPECT_EQ(rich.getCertificate().maximum_template_depth, 2);

    const auto changed = TemplateChecker::checkAll({representativeCanonicalIRDefinition('C')});
    EXPECT_NE(rich.getDefinitionHash(), changed.front()->getDefinitionHash());

    const auto recursive = TemplateChecker::checkAll({recursiveDefinition("Recursive", 0x101)});
    ASSERT_EQ(recursive.size(), 1);
    EXPECT_EQ(recursive.front()->getCheckerABI(), 2);
    EXPECT_EQ(recursive.front()->getCertificate().logical_node_count, 4);
    EXPECT_EQ(recursive.front()->getCertificate().maximum_template_depth, 2);
}

TEST(UDTTemplateChecker, CanonicalizesDependenciesAndInputOrderDeterministically)
{
    auto caller_a = aliasDefinition("Caller", 0x30);
    caller_a.nodes.clear();
    auto root_a = builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}});
    auto call_20_a = node(TemplateNodeKind::DefinitionCall);
    call_20_a.dependency_ordinal = 0;
    call_20_a.children.push_back({.reference = 0, .label = {}});
    auto call_10_a = node(TemplateNodeKind::DefinitionCall);
    call_10_a.dependency_ordinal = 1;
    call_10_a.children.push_back({.reference = 0, .label = {}});
    caller_a.nodes = {std::move(root_a), std::move(call_20_a), std::move(call_10_a)};
    caller_a.dependencies = {
        {.type_uuid = testUUID(0x20), .revision = 1, .target_definition_hash = {}},
        {.type_uuid = testUUID(0x10), .revision = 1, .target_definition_hash = {}},
    };

    auto caller_b = aliasDefinition("Caller", 0x30);
    caller_b.nodes.clear();
    auto root_b = builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}});
    auto call_20_b = node(TemplateNodeKind::DefinitionCall);
    call_20_b.dependency_ordinal = 1;
    call_20_b.children.push_back({.reference = 0, .label = {}});
    auto call_10_b = node(TemplateNodeKind::DefinitionCall);
    call_10_b.dependency_ordinal = 0;
    call_10_b.children.push_back({.reference = 0, .label = {}});
    caller_b.nodes = {std::move(root_b), std::move(call_20_b), std::move(call_10_b)};
    caller_b.dependencies = {
        {.type_uuid = testUUID(0x10), .revision = 1, .target_definition_hash = {}},
        {.type_uuid = testUUID(0x20), .revision = 1, .target_definition_hash = {}},
    };

    auto first = TemplateChecker::checkAll({std::move(caller_a), aliasDefinition("Leaf20", 0x20), aliasDefinition("Leaf10", 0x10)});
    auto second = TemplateChecker::checkAll({aliasDefinition("Leaf10", 0x10), std::move(caller_b), aliasDefinition("Leaf20", 0x20)});

    const auto & first_caller = byName(first, "Caller");
    const auto & second_caller = byName(second, "Caller");
    EXPECT_EQ(first_caller.getCertificate(), second_caller.getCertificate());
    ASSERT_EQ(first_caller.getDependencies().size(), 2);
    EXPECT_EQ(first_caller.getDependencies()[0].type_uuid, testUUID(0x10));
    EXPECT_EQ(first_caller.getDependencies()[1].type_uuid, testUUID(0x20));
    EXPECT_NE(first_caller.getDependencies()[0].target_definition_hash, Digest{});
    EXPECT_EQ(first_caller.getDependencies()[0].target_definition_hash, byName(first, "Leaf10").getDefinitionHash());
    EXPECT_EQ(first_caller.getDependencies()[1].target_definition_hash, byName(first, "Leaf20").getDefinitionHash());
}

TEST(UDTTemplateChecker, DependencyHashesAreDerivedOrMustMatchExactly)
{
    const auto leaf_only = TemplateChecker::checkAll({aliasDefinition("Leaf", 0x40)});
    const Digest expected_hash = leaf_only.front()->getDefinitionHash();

    auto exact = callDefinition("Caller", 0x41, 0x40);
    exact.dependencies[0].target_definition_hash = expected_hash;
    const auto accepted = TemplateChecker::checkAll({std::move(exact), aliasDefinition("Leaf", 0x40)});
    EXPECT_EQ(byName(accepted, "Caller").getDependencies()[0].target_definition_hash, expected_hash);

    auto mismatch = callDefinition("Caller", 0x41, 0x40);
    mismatch.dependencies[0].target_definition_hash = expected_hash;
    mismatch.dependencies[0].target_definition_hash[0] ^= 0xff;
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(mismatch), aliasDefinition("Leaf", 0x40)}); });
}

TEST(UDTTemplateChecker, RejectsIdentityNameAndBuiltInAuthorityViolations)
{
    const TemplateChecker::BuiltInFamilyAuthorityForTest missing_collision_authority{
        .is_registered_family = isRegisteredBuiltInFamilyForTest,
    };
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS,
        [&]
        {
            return TemplateChecker::checkAllWithBuiltInFamilyAuthorityForTest(
                {scalarDefinition("MissingCollisionAuthority", 0x4f0)}, missing_collision_authority);
        });

    const TemplateChecker::BuiltInFamilyAuthorityForTest missing_registration_authority{
        .collides_with_registered_family_or_alias = collidesWithRegisteredBuiltInFamilyOrAliasForTest,
    };
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS,
        [&]
        {
            return TemplateChecker::checkAllWithBuiltInFamilyAuthorityForTest(
                {scalarDefinition("MissingRegistrationAuthority", 0x4f1)}, missing_registration_authority);
        });

    auto collision = scalarDefinition("Array", 0x50);
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(collision)}); });

    auto folded_collision = scalarDefinition("uInT64", 0x501);
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: definition name collides with a registered built-in family or alias",
        [&] { return TemplateChecker::checkAll({std::move(folded_collision)}); });

    auto qualified_creator_collision = scalarDefinition("db.UInt64", 0x5011);
    qualified_creator_collision.normalized_local_name = "UInt64";
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: definition name collides with a registered built-in family or alias",
        [&] { return TemplateChecker::checkAll({std::move(qualified_creator_collision)}); });

    auto qualified_alias_collision = scalarDefinition("db.vArChAr", 0x5012);
    qualified_alias_collision.normalized_local_name = "vArChAr";
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: definition name collides with a registered built-in family or alias",
        [&] { return TemplateChecker::checkAll({std::move(qualified_alias_collision)}); });

    auto unstructured_qualified_name = scalarDefinition("db.SafeType", 0x5013);
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: qualified definition name lacks a structured local identifier",
        [&] { return TemplateChecker::checkAll({std::move(unstructured_qualified_name)}); });

    auto safe_database_component = scalarDefinition("Array.SafeType", 0x5014);
    safe_database_component.normalized_local_name = "SafeType";
    const auto safe_database_checked = TemplateChecker::checkAll({std::move(safe_database_component)});
    ASSERT_EQ(safe_database_checked.size(), 1);
    EXPECT_EQ(safe_database_checked.front()->getNormalizedName(), "Array.SafeType");
    EXPECT_EQ(safe_database_checked.front()->getNormalizedLocalName(), "SafeType");

    auto quoted_dot_local = scalarDefinition("db.`Safe.Name`", 0x5015);
    quoted_dot_local.normalized_local_name = "Safe.Name";
    const auto quoted_dot_checked = TemplateChecker::checkAll({std::move(quoted_dot_local)});
    ASSERT_EQ(quoted_dot_checked.size(), 1);
    EXPECT_EQ(quoted_dot_checked.front()->getNormalizedLocalName(), "Safe.Name");

    auto duplicate_local_first = scalarDefinition("db1.Shared", 0x5016);
    duplicate_local_first.normalized_local_name = "Shared";
    auto duplicate_local_second = scalarDefinition("db2.Shared", 0x5017);
    duplicate_local_second.normalized_local_name = "Shared";
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: duplicate normalized definition name",
        [&] { return TemplateChecker::checkAll({std::move(duplicate_local_first), std::move(duplicate_local_second)}); });

    /// The collision spelling is reserved for UDT names but is not promoted to
    /// an executable built-in spelling when the factory family is case-sensitive.
    auto invalid_folded_atom = scalarDefinition("SafeName", 0x502, "uInT64");
    expectExactDBError(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid user-defined type definition: built-in node is noncanonical or names an unregistered family",
        [&] { return TemplateChecker::checkAll({std::move(invalid_folded_atom)}); });

    const auto folded_alias = TemplateChecker::checkAll({scalarDefinition("AliasAtom", 0x503, "vArChAr")});
    ASSERT_EQ(folded_alias.size(), 1);

    auto unknown_family = scalarDefinition("UnknownFamily", 0x51, "NotRegistered");
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(unknown_family)}); });

    auto unknown_kind = scalarDefinition("UnknownKind", 0x52);
    unknown_kind.nodes[0] = node(static_cast<TemplateNodeKind>(255));
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(unknown_kind)}); });

    auto bad_capability = scalarDefinition("BadCapability", 0x53);
    bad_capability.semantic_capabilities = 0x80;
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(bad_capability)}); });

    auto same_identity_a = scalarDefinition("First", 0x54);
    auto same_identity_b = scalarDefinition("Second", 0x54);
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(same_identity_a), std::move(same_identity_b)}); });

    auto same_name_a = scalarDefinition("Same", 0x55);
    auto same_name_b = scalarDefinition("Same", 0x56);
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(same_name_a), std::move(same_name_b)}); });
}

TEST(UDTTemplateChecker, RejectsBadKindsArityAndEveryUncertifiedRecursionForm)
{
    auto value_root = scalarDefinition("ValueRoot", 0x5f);
    value_root.nodes = {node(TemplateNodeKind::UnsignedLiteral)};
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(value_root)}); });

    auto wrong_kind = callDefinition("Caller", 0x60, 0x61, ParameterKind::UInt64);
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(wrong_kind), aliasDefinition("Leaf", 0x61)}); });

    auto wrong_arity = callDefinition("Caller", 0x62, 0x63, ParameterKind::Type, false);
    expectDBError(
        ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(wrong_arity), aliasDefinition("Leaf", 0x63)}); });

    auto value_branch = recursiveDefinition("ValueBranch", 0x631);
    value_branch.nodes[1] = node(TemplateNodeKind::UnsignedLiteral);
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(value_branch)}); });

    auto unguarded = recursiveDefinition("Unguarded", 0x64);
    unguarded.nodes = {node(TemplateNodeKind::SelfCall)};
    unguarded.nodes[0].parameter = 1;
    unguarded.nodes[0].decrement = 1;
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(unguarded)}); });

    auto policy_recursive = recursiveDefinition("PolicyRecursive", 0x65);
    policy_recursive.policy_bearing = true;
    policy_recursive.policy_semantic_hash[0] = 1;
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(policy_recursive)}); });

    auto first = callDefinition("First", 0x66, 0x67);
    auto second = callDefinition("Second", 0x67, 0x66);
    expectDBError(ErrorCodes::BAD_ARGUMENTS, [&] { return TemplateChecker::checkAll({std::move(first), std::move(second)}); });
}

TEST(UDTTemplateChecker, EnforcesProspectiveBoundsAndDoesNotPublishStatisticsOnFailure)
{
    TemplateCheckerStatistics successful;
    const auto baseline = TemplateChecker::checkAll({scalarDefinition("Bounded", 0x70)}, {}, &successful);
    ASSERT_EQ(baseline.size(), 1);
    ASSERT_GT(successful.maximum_definition_input_bytes, 0);
    EXPECT_EQ(successful.checked_definitions, 1);
    EXPECT_GT(successful.charged_work, 0);
    EXPECT_GT(successful.canonical_bytes, 0);
    EXPECT_GT(successful.scratch_peak_bytes, 0);

    TemplateCheckerLimits bytes_limit;
    bytes_limit.maximum_definition_input_bytes = successful.maximum_definition_input_bytes - 1;
    TemplateCheckerStatistics untouched{
        .accepted_input_bytes = 111,
        .maximum_definition_input_bytes = 222,
        .checked_definitions = 333,
        .graph_edges = 444,
        .charged_work = 555,
        .canonical_bytes = 666,
        .scratch_peak_bytes = 777,
    };
    expectDBError(
        ErrorCodes::TOO_MANY_BYTES,
        [&] { return TemplateChecker::checkAll({scalarDefinition("Bounded", 0x70)}, bytes_limit, &untouched); });
    EXPECT_EQ(untouched.accepted_input_bytes, 111);
    EXPECT_EQ(untouched.checked_definitions, 333);
    EXPECT_EQ(untouched.scratch_peak_bytes, 777);

    TemplateCheckerLimits formal_name_limit;
    formal_name_limit.maximum_formal_name_bytes = 0;
    expectDBError(
        ErrorCodes::TOO_MANY_BYTES, [&] { return TemplateChecker::checkAll({aliasDefinition("LongFormal", 0x701)}, formal_name_limit); });

    TemplateCheckerLimits enum_limit;
    enum_limit.maximum_ir_enum_entries = 1;
    expectDBError(
        ErrorCodes::TOO_MANY_BYTES,
        [&] { return TemplateChecker::checkAll({representativeCanonicalIRDefinition('B')}, enum_limit); });

    auto depth_two = scalarDefinition("DepthTwo", 0x71);
    depth_two.nodes = {builtIn("Array", {{.reference = 1, .label = {}}}), builtIn("UInt64")};
    TemplateCheckerLimits depth_limit;
    depth_limit.maximum_template_depth = 1;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { return TemplateChecker::checkAll({std::move(depth_two)}, depth_limit); });

    auto shared = scalarDefinition("Shared", 0x72);
    shared.nodes = {builtIn("Tuple", {{.reference = 1, .label = {}}, {.reference = 1, .label = {}}}), builtIn("UInt64")};
    TemplateCheckerLimits occurrence_limit;
    occurrence_limit.maximum_logical_node_occurrences = 2;
    expectDBError(ErrorCodes::TOO_MANY_BYTES, [&] { return TemplateChecker::checkAll({std::move(shared)}, occurrence_limit); });
}

}
}
