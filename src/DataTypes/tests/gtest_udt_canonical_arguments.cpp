#include <DataTypes/UDT/CanonicalTypeArguments.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TemplateSpecializer.h>

#include <Common/Exception.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/tests/gtest_global_register.h>

#include <Core/Field.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <array>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace DB::UDT
{
namespace
{

String toHex(std::string_view bytes)
{
    constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(bytes.size() * 2);
    for (const unsigned char byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

String fromBytes(std::initializer_list<UInt8> bytes)
{
    String result;
    result.reserve(bytes.size());
    for (const UInt8 byte : bytes)
        result.push_back(static_cast<char>(byte));
    return result;
}

void appendVarUIntForTest(String & output, UInt64 value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

String wrapTypeFrame(std::string_view type_encoding)
{
    String result = fromBytes({1, 0, 1, 1});
    appendVarUIntForTest(result, type_encoding.size());
    result.append(type_encoding);
    return result;
}

ASTPtr parseType(const String & text)
{
    tryRegisterAggregateFunctions();
    ParserDataType parser;
    return parseQuery(parser, text, "canonical TYPE argument test", 0, 150, 0);
}

CanonicalTypeArguments makeSingleTypeArguments(const String & type_name)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(CanonicalTypeArgumentValue::type(parseType(type_name)));
    return CanonicalTypeArguments::validate(parameters, std::move(values));
}

std::vector<Field> frozenFieldInventory()
{
    Object object;
    object.emplace("key", Field(UInt64{1}));
    return {
        Field(Null{}),
        Field(NEGATIVE_INFINITY),
        Field(POSITIVE_INFINITY),
        Field(UInt64{42}),
        Field(Int64{-42}),
        Field(UInt128{42}),
        Field(Int128{-42}),
        Field(UInt256{42}),
        Field(Int256{-42}),
        Field(Float64{-0.0}),
        Field(String{"a\0b", 3}),
        Field(true),
        Field(DecimalField<Decimal32>(42, 3)),
        Field(DecimalField<Decimal64>(42, 3)),
        Field(DecimalField<Decimal128>(Int128{42}, 3)),
        Field(DecimalField<Decimal256>(Int256{42}, 3)),
        Field(UUID{42}),
        Field(IPv4{42}),
        Field(IPv6{42}),
        Field(Array{Field(UInt64{1}), Field(String{"two"})}),
        Field(Tuple{Field(true), Field(Int64{-2})}),
        Field(Map{Field(Tuple{Field(UInt64{1}), Field(String{"one"})})}),
        Field(std::move(object)),
        Field(AggregateFunctionStateData{.name = "AggregateFunction(sum, UInt64)", .data = String{"x\0y", 3}}),
    };
}

String encodeUnknownAggregateParameter(const Field & parameter)
{
    WriteBufferFromOwnString output;
    output.write(static_cast<UInt8>(BinaryTypeIndex::AggregateFunction));
    writeVarUInt(1, output);
    writeStringBinary("__u", output);
    writeVarUInt(1, output);
    encodeField(parameter, output);
    writeVarUInt(1, output);
    output.write(static_cast<UInt8>(BinaryTypeIndex::UInt64));
    return output.str();
}

UUID canonicalArgumentTestUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

struct SpecializationObservation
{
    IASTHash tree_hash;
    String physical_name;
    TemplateSpecializerStatistics statistics;

    bool operator==(const SpecializationObservation &) const = default;
};

SpecializationObservation specializeIdentityType(
    const IAuthorityAdapter & authority,
    const Definition & definition,
    const CanonicalTypeArguments & arguments)
{
    auto attempt = TemplateSpecializer::Attempt::begin(authority);
    const TemplateSpecializationID id = attempt.specialize(definition.getIdentity(), arguments);
    auto finished = attempt.finish();
    if (id >= finished.specializations.size())
        throw std::logic_error("specialization ID is outside the committed test batch");
    const ASTPtr & physical_ast = finished.specializations[id].canonical_physical_ast;
    return {
        .tree_hash = physical_ast->getTreeHash(false),
        .physical_name = DataTypeFactory::instance().get(physical_ast)->getName(),
        .statistics = finished.statistics,
    };
}

std::pair<Definition::Ptr, AuthorityAdapterPtr> makeIdentityTypeAuthority()
{
    const UUID database_uuid = canonicalArgumentTestUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
    DefinitionInput input;
    input.identity = {
        .database_uuid = database_uuid,
        .type_uuid = canonicalArgumentTestUUID(0x1000000000000000ULL, 0x43414e4f4e494341ULL),
        .revision = 1,
    };
    input.normalized_name = "CanonicalArgumentIdentity";
    input.parameters = {{.normalized_name = "T", .kind = ParameterKind::Type}};
    TemplateNode formal;
    formal.kind = TemplateNodeKind::TypeParameter;
    input.nodes = {std::move(formal)};

    auto definition = TemplateChecker::checkAll({std::move(input)}).front();
    TypeAuthorityCapabilities capabilities;
    capabilities.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    capabilities.limits = {
        .maximum_definitions = 1,
        .maximum_definition_bytes = 1ULL << 20,
        .maximum_template_nodes = 16,
        .maximum_direct_dependencies = 1,
        .maximum_transitive_dependencies = 1,
        .maximum_checker_work = 1'024,
    };
    auto authority = makeTransientAuthorityAdapter(database_uuid, capabilities, {definition});
    return {std::move(definition), std::move(authority)};
}

ASTPtr aggregateFunctionTypeWithParameter(Field parameter, String function_name = "__u")
{
    auto function = makeASTFunction(function_name, make_intrusive<ASTLiteral>(std::move(parameter)));
    return makeASTDataType("AggregateFunction", function, makeASTDataType("UInt64"));
}

template <typename F>
void expectDBError(int expected_code, F && action)
{
    try
    {
        action();
        FAIL() << "expected DB::Exception with code " << expected_code;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << exception.displayText();
    }
}

}

static_assert(!std::is_constructible_v<CanonicalTypeArgument, DataTypePtr, String, String>);

TEST(UDTCanonicalArguments, FactoryTypeAndTypedValuesHaveOneStableEncoding)
{
    const std::vector<Parameter> parameters{
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::Int8},
        {.normalized_name = "S", .kind = ParameterKind::String},
    };
    String binary_string{"a\0b", 3};
    std::vector<CanonicalTypeArgumentValue> actuals;
    actuals.push_back(CanonicalTypeArgumentValue::type(parseType("UInt64")));
    actuals.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int8, -1));
    actuals.push_back(CanonicalTypeArgumentValue::string(binary_string));

    const auto canonical = CanonicalTypeArguments::validate(parameters, std::move(actuals));
    EXPECT_EQ(toHex(canonical.encoded()), "01000301010407ff0b03610062");
}

TEST(UDTCanonicalArguments, TypeIdentityUsesFullBinaryEncoding)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};

    auto make = [&](const String & type)
    {
        std::vector<CanonicalTypeArgumentValue> values;
        values.push_back(CanonicalTypeArgumentValue::type(parseType(type)));
        return CanonicalTypeArguments::validate(parameters, std::move(values));
    };

    EXPECT_EQ(make("Dynamic(max_types=7)"), make("Dynamic(max_types = 7)"));
    EXPECT_NE(make("Dynamic(max_types=7)"), make("Dynamic(max_types=8)"));
}

TEST(UDTCanonicalArguments, AdmissionOwnsTheASTAndCanonicalizesEnumPrivately)
{
    ASTPtr caller_owned = parseType("Array(Enum('first', 'second'))");
    const auto * array = caller_owned->as<ASTDataType>();
    ASSERT_NE(array, nullptr);
    const auto * array_arguments = array->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(array_arguments, nullptr);
    const auto * source_enum = array_arguments->children.front()->as<ASTDataType>();
    ASSERT_NE(source_enum, nullptr);
    const auto * source_enum_arguments = source_enum->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(source_enum_arguments, nullptr);
    ASSERT_NE(source_enum_arguments->children.front()->as<ASTLiteral>(), nullptr);

    CanonicalTypeArgumentAdmissionStatistics statistics;
    const auto admitted = CanonicalTypeArgument::fromFactoryValidatedAST(caller_owned, {}, &statistics);

    EXPECT_EQ(admitted.getCanonicalName(), "Array(Enum8('first' = 1, 'second' = 2))");
    EXPECT_EQ(statistics.factory_calls, 1);
    EXPECT_EQ(statistics.generic_enums_canonicalized, 1);
    EXPECT_GT(statistics.ast_node_occurrences, 1);
    /// Pre-factory normalization and factory validation never touch the caller.
    EXPECT_NE(source_enum_arguments->children.front()->as<ASTLiteral>(), nullptr);
}

TEST(UDTCanonicalArguments, GenericEnumIsCanonicalizedBeforeFactoryWithoutMutatingCaller)
{
    ASTPtr caller_owned = parseType("Array(eNuM('wide' = -129, 'next'))");
    const auto * array = caller_owned->as<ASTDataType>();
    ASSERT_NE(array, nullptr);
    const auto * arguments = array->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(arguments, nullptr);
    const auto * source_enum = arguments->children.front()->as<ASTDataType>();
    ASSERT_NE(source_enum, nullptr);
    const auto * source_entries = source_enum->getArguments()->as<ASTExpressionList>();
    ASSERT_NE(source_entries, nullptr);
    ASSERT_NE(source_entries->children.back()->as<ASTLiteral>(), nullptr);

    CanonicalTypeArgumentAdmissionStatistics statistics;
    const auto admitted = CanonicalTypeArgument::fromFactoryValidatedAST(caller_owned, {}, &statistics);

    EXPECT_EQ(admitted.getCanonicalName(), "Array(Enum16('wide' = -129, 'next' = -128))");
    EXPECT_EQ(statistics.generic_enums_canonicalized, 1);
    EXPECT_EQ(statistics.factory_calls, 1);
    EXPECT_NE(source_entries->children.back()->as<ASTLiteral>(), nullptr);
}

TEST(UDTCanonicalArguments, AdmissionCoversTheCurrentParserDataTypeNodeSurface)
{
    const std::vector<String> types{
        "Tuple(named Array(Enum('a', 'b')), plain Nullable(String))",
        "Nested(id UInt64, payload String)",
        "Dynamic(max_types=7)",
        "AggregateFunction(quantiles(0.5, 0.9), UInt64)",
        "AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))",
        "JSON(max_dynamic_paths=10, path Enum('x', 'y'), SKIP hidden.path, SKIP REGEXP '^tmp')",
    };

    for (const auto & type : types)
    {
        SCOPED_TRACE(type);
        CanonicalTypeArgumentAdmissionStatistics statistics;
        EXPECT_NO_THROW(static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(parseType(type), {}, &statistics)));
        EXPECT_EQ(statistics.factory_calls, 1);
        EXPECT_GT(statistics.ast_node_occurrences, 0);
    }
}

TEST(UDTCanonicalArguments, HiddenIdentifierSemanticStateIsRejectedBeforeCloneOrFactory)
{
    auto identifier = make_intrusive<ASTIdentifier>(std::vector<String>{String(256, 'h'), "sum"});
    identifier->setShortName("sum");
    ASSERT_FALSE(identifier->getParserIdentifierSemanticStringBytes().has_value());
    ASTPtr type = makeASTDataType("AggregateFunction", identifier, makeASTDataType("UInt64"));
    CanonicalTypeArgumentAdmissionStatistics unchanged;
    unchanged.ast_node_occurrences = 777;
    unchanged.factory_calls = 777;

    expectDBError(
        ErrorCodes::BAD_ARGUMENTS, [&] { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(type, {}, &unchanged)); });

    EXPECT_EQ(unchanged.ast_node_occurrences, 777);
    EXPECT_EQ(unchanged.factory_calls, 777);
}

TEST(UDTCanonicalArguments, ParserCompoundIdentifierStateAndCloneBytesAreAdmittedExactly)
{
    const std::array cases{
        std::pair{String("hidden"), size_t{0}},
        std::pair{String("hidden.path"), String("hidden").size()},
    };
    for (const auto & [path, expected_semantic_bytes] : cases)
    {
        SCOPED_TRACE(path);
        ASTPtr type = parseType("JSON(SKIP " + path + ")");
        const auto * json = type->as<ASTDataType>();
        ASSERT_NE(json, nullptr);
        const auto * arguments = json->getArguments()->as<ASTExpressionList>();
        ASSERT_NE(arguments, nullptr);
        ASSERT_EQ(arguments->children.size(), 1);
        const auto * object_argument = arguments->children.front()->as<ASTObjectTypeArgument>();
        ASSERT_NE(object_argument, nullptr);
        const auto * identifier = object_argument->skip_path->as<ASTIdentifier>();
        ASSERT_NE(identifier, nullptr);
        const auto semantic_string_bytes = identifier->getParserIdentifierSemanticStringBytes();
        ASSERT_TRUE(semantic_string_bytes.has_value());
        EXPECT_EQ(*semantic_string_bytes, expected_semantic_bytes);

        EXPECT_NO_THROW(static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(type)));
    }

    CanonicalTypeArgumentLimits limits;
    /// JSON + full_name + both parts consume exactly 25 visible bytes. The
    /// compound parser's clone-owned semantic table must consume six more.
    limits.maximum_owned_string_bytes = 25;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&] { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(parseType("JSON(SKIP hidden.path)"), limits)); });
}

TEST(UDTCanonicalArguments, CompositeAggregateParameterIsBoundedAndEncodedExactly)
{
    CanonicalTypeArgumentAdmissionStatistics first_statistics;
    const auto first = CanonicalTypeArgument::fromFactoryValidatedAST(
        parseType("AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))"), {}, &first_statistics);
    const auto same = CanonicalTypeArgument::fromFactoryValidatedAST(
        parseType("AggregateFunction(1, sumMapFiltered( [1,4,8] ), Array(UInt64), Array(UInt64))"));
    const auto different = CanonicalTypeArgument::fromFactoryValidatedAST(
        parseType("AggregateFunction(1, sumMapFiltered([1, 4, 9]), Array(UInt64), Array(UInt64))"));

    EXPECT_EQ(first, same);
    EXPECT_NE(first, different);
    EXPECT_EQ(first.getCanonicalName(), "AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))");
    EXPECT_EQ(first_statistics.field_node_occurrences, 5); /// Version plus Array root and its three elements.
    EXPECT_EQ(first_statistics.field_edges, 3);
    EXPECT_EQ(first_statistics.maximum_field_depth, 2);
    EXPECT_EQ(first_statistics.factory_calls, 1);
}

TEST(UDTCanonicalArguments, PreflightAdmitsTheFrozenCanonicalFieldInventoryBeforeFactory)
{
    const auto inventory = frozenFieldInventory();

    for (const auto & field : inventory)
    {
        SCOPED_TRACE(fieldTypeToString(field.getType()));
        /// UNKNOWN_AGGREGATE_FUNCTION proves the structured parameter crossed
        /// canonical preflight and reached the factory boundary.
        expectDBError(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            [&] { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(aggregateFunctionTypeWithParameter(field))); });
    }

    expectDBError(
        ErrorCodes::BAD_ARGUMENTS,
        [&]
        { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(aggregateFunctionTypeWithParameter(Field(CustomType{})))); });
}

TEST(UDTCanonicalArguments, FieldResourceLimitsRejectProspectivelyBeforeFactory)
{
    const Field composite(Array{Field(Array{Field(UInt64{1}), Field(UInt64{4}), Field(UInt64{8})})});

    const auto rejected = [&](auto set_limit)
    {
        CanonicalTypeArgumentLimits limits;
        set_limit(limits);
        expectDBError(
            ErrorCodes::LIMIT_EXCEEDED,
            [&]
            { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(aggregateFunctionTypeWithParameter(composite), limits)); });
    };

    rejected([](auto & limits) { limits.maximum_field_nodes = 4; });
    rejected([](auto & limits) { limits.maximum_field_edges = 3; });
    rejected([](auto & limits) { limits.maximum_field_depth = 2; });

    CanonicalTypeArgumentLimits string_limits;
    string_limits.maximum_owned_string_bytes = 40;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        {
            static_cast<void>(
                CanonicalTypeArgument::fromFactoryValidatedAST(aggregateFunctionTypeWithParameter(Field(String(32, 'x'))), string_limits));
        });
}

TEST(UDTCanonicalArguments, SpecializedDataTypeNodesAreValidRoots)
{
    const ASTPtr tuple = parseType("Tuple(key Array(Enum('a', 'b')), value Nullable(String))");
    const ASTPtr enum_type = parseType("Enum8('zero' = 0, 'one' = 1)");
    ASSERT_NE(tuple->as<ASTTupleDataType>(), nullptr);
    ASSERT_NE(enum_type->as<ASTEnumDataType>(), nullptr);

    EXPECT_EQ(
        CanonicalTypeArgument::fromFactoryValidatedAST(tuple).getCanonicalName(),
        "Tuple(key Array(Enum8('a' = 1, 'b' = 2)), value Nullable(String))");
    EXPECT_EQ(CanonicalTypeArgument::fromFactoryValidatedAST(enum_type).getCanonicalName(), "Enum8('zero' = 0, 'one' = 1)");
}

TEST(UDTCanonicalArguments, StructuralLimitsAndUnknownFamiliesFailBeforeAResult)
{
    auto nested = parseType("Array(Array(UInt64))");
    CanonicalTypeArgumentLimits limits;
    limits.maximum_ast_depth = 2;
    EXPECT_THROW(CanonicalTypeArgument::fromFactoryValidatedAST(nested, limits), Exception);

    auto unknown = makeASTDataType("DefinitelyNotARegisteredType");
    EXPECT_THROW(CanonicalTypeArgument::fromFactoryValidatedAST(unknown), Exception);
}

TEST(UDTCanonicalArguments, KindRangeAndByteLimitsFailBeforeResult)
{
    const std::vector<Parameter> parameters{{.normalized_name = "N", .kind = ParameterKind::UInt8}};

    EXPECT_THROW(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt8, 256), Exception);

    std::vector<CanonicalTypeArgumentValue> wrong_kind;
    wrong_kind.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int8, 1));
    EXPECT_THROW(CanonicalTypeArguments::validate(parameters, std::move(wrong_kind)), Exception);

    std::vector<CanonicalTypeArgumentValue> too_large;
    too_large.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt8, 1));
    EXPECT_THROW(CanonicalTypeArguments::validate(parameters, std::move(too_large), 3, 16), Exception);

    const std::vector<Parameter> no_parameters;
    EXPECT_THROW(CanonicalTypeArguments::validate(no_parameters, {}, 2, 16), Exception);
    EXPECT_EQ(toHex(CanonicalTypeArguments::validate(no_parameters, {}, 3, 16).encoded()), "010000");
}

TEST(UDTCanonicalArguments, DecoderRoundTripsEveryFormalKind)
{
    const std::vector<Parameter> parameters{
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "B", .kind = ParameterKind::Bool},
        {.normalized_name = "U8", .kind = ParameterKind::UInt8},
        {.normalized_name = "U16", .kind = ParameterKind::UInt16},
        {.normalized_name = "U32", .kind = ParameterKind::UInt32},
        {.normalized_name = "U64", .kind = ParameterKind::UInt64},
        {.normalized_name = "I8", .kind = ParameterKind::Int8},
        {.normalized_name = "I16", .kind = ParameterKind::Int16},
        {.normalized_name = "I32", .kind = ParameterKind::Int32},
        {.normalized_name = "I64", .kind = ParameterKind::Int64},
        {.normalized_name = "S", .kind = ParameterKind::String},
    };
    std::vector<CanonicalTypeArgumentValue> values;
    values.push_back(
        CanonicalTypeArgumentValue::type(
            parseType("Tuple(named Array(Enum('first', 'second')), cfg JSON(max_dynamic_paths=10, path UInt32, SKIP hidden.path))")));
    values.push_back(CanonicalTypeArgumentValue::boolean(true));
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt8, std::numeric_limits<UInt8>::max()));
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt16, std::numeric_limits<UInt16>::max()));
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt32, std::numeric_limits<UInt32>::max()));
    values.push_back(CanonicalTypeArgumentValue::unsignedInteger(ParameterKind::UInt64, std::numeric_limits<UInt64>::max()));
    values.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int8, std::numeric_limits<Int8>::min()));
    values.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int16, std::numeric_limits<Int16>::min()));
    values.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int32, std::numeric_limits<Int32>::min()));
    values.push_back(CanonicalTypeArgumentValue::signedInteger(ParameterKind::Int64, std::numeric_limits<Int64>::min()));
    values.push_back(CanonicalTypeArgumentValue::string(String{"binary\0string", 13}));

    const auto encoded = CanonicalTypeArguments::validate(parameters, std::move(values));
    const auto decoded = CanonicalTypeArguments::decode(parameters, encoded.encoded());

    EXPECT_EQ(decoded, encoded);
    ASSERT_EQ(decoded.values().size(), parameters.size());
    EXPECT_EQ(
        std::get<CanonicalTypeArgument>(decoded.values().front().value).getCanonicalName(),
        std::get<CanonicalTypeArgument>(encoded.values().front().value).getCanonicalName());
}

TEST(UDTCanonicalArguments, DecoderRoundTripsTheAdmittedTypeSurface)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    const std::array types{
        "UInt64",
        "DateTime('Europe/Berlin')",
        "DateTime64(6, 'Europe/Berlin')",
        "Time64(6)",
        "FixedString(17)",
        "Decimal(7, 3)",
        "Enum16('negative' = -1000, 'positive' = 1000)",
        "Tuple(named Array(Enum8('a' = 1, 'b' = 2)), plain Nullable(String))",
        "QBit(Float32, 64, 16)",
        "IntervalNanosecond",
        "LowCardinality(String)",
        "Map(String, UInt64)",
        "Variant(String, UInt32, Date32)",
        "Dynamic(max_types=7)",
        "AggregateFunction(1, quantiles(0.5, 0.9), UInt64)",
        "SimpleAggregateFunction(sum, UInt64)",
        "Nested(id UInt64, payload String)",
        "JSON(max_dynamic_paths=10, max_dynamic_types=7, a.path UInt32, SKIP hidden.path, SKIP REGEXP '^tmp')",
        "Bool",
        "Array(Point)",
    };

    for (const auto * type_name : types)
    {
        SCOPED_TRACE(type_name);
        const auto encoded = makeSingleTypeArguments(type_name);
        EXPECT_EQ(CanonicalTypeArguments::decode(parameters, encoded.encoded()), encoded);
    }
}

TEST(UDTCanonicalArguments, FreshAndDecodedTypesSpecializeFromTheSameCanonicalAST)
{
    const auto [definition, authority] = makeIdentityTypeAuthority();
    for (const auto * type_name : {"Dynamic", "Decimal32(3)", "AggregateFunction(sum, UInt64)"})
    {
        SCOPED_TRACE(type_name);
        const auto fresh = makeSingleTypeArguments(type_name);
        const auto decoded = CanonicalTypeArguments::decode(definition->getParameters(), fresh.encoded());
        ASSERT_EQ(decoded, fresh);
        EXPECT_EQ(specializeIdentityType(*authority, *definition, fresh), specializeIdentityType(*authority, *definition, decoded));
    }
}

TEST(UDTCanonicalArguments, FrozenCustomDomainNamesRoundTripExactly)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    for (const auto * type_name : {"Point", "Ring", "Polygon", "MultiPoint", "LineString", "MultiLineString", "MultiPolygon", "Geometry"})
    {
        SCOPED_TRACE(type_name);
        const auto encoded = makeSingleTypeArguments(type_name);
        EXPECT_EQ(CanonicalTypeArguments::decode(parameters, encoded.encoded()), encoded);
    }
}

TEST(UDTCanonicalArguments, DecoderAdmitsTheFrozenFieldInventoryBeforeFactory)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    for (const auto & field : frozenFieldInventory())
    {
        SCOPED_TRACE(fieldTypeToString(field.getType()));
        expectDBError(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            [&] { static_cast<void>(CanonicalTypeArguments::decode(parameters, wrapTypeFrame(encodeUnknownAggregateParameter(field)))); });
    }
}

TEST(UDTCanonicalArguments, DecoderRejectsMalformedFramingKindsAndLimits)
{
    const std::vector<Parameter> none;
    EXPECT_THROW(CanonicalTypeArguments::decode(none, fromBytes({2, 0, 0})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(none, fromBytes({1})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(none, fromBytes({1, 0, 0x80, 0})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(none, fromBytes({1, 0, 1})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(none, fromBytes({1, 0, 0, 0})), Exception);

    const std::vector<Parameter> boolean{{.normalized_name = "B", .kind = ParameterKind::Bool}};
    const String valid_bool = fromBytes({1, 0, 1, 2, 1});
    EXPECT_EQ(CanonicalTypeArguments::decode(boolean, valid_bool).encoded(), valid_bool);
    EXPECT_THROW(CanonicalTypeArguments::decode(boolean, fromBytes({1, 0, 1, 0})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(boolean, fromBytes({1, 0, 1, 12})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(boolean, fromBytes({1, 0, 1, 2, 2})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(boolean, valid_bool, 4, 16), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(boolean, valid_bool, 64, 1), Exception);

    const std::vector<Parameter> two_booleans{
        {.normalized_name = "A", .kind = ParameterKind::Bool},
        {.normalized_name = "B", .kind = ParameterKind::Bool},
    };
    EXPECT_THROW(CanonicalTypeArguments::decode(two_booleans, fromBytes({1, 0, 2})), Exception);

    const std::vector<Parameter> integer{{.normalized_name = "N", .kind = ParameterKind::UInt64}};
    EXPECT_THROW(CanonicalTypeArguments::decode(integer, fromBytes({1, 0, 1, 6, 1, 2, 3})), Exception);

    const std::vector<Parameter> string{{.normalized_name = "S", .kind = ParameterKind::String}};
    EXPECT_THROW(CanonicalTypeArguments::decode(string, fromBytes({1, 0, 1, 11, 0x80, 0})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(string, fromBytes({1, 0, 1, 11, 5, 'a'})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(string, fromBytes({1, 0, 1, 11, 5, 'a', 'b', 'c', 'd', 'e'}), 64, 4), Exception);
}

TEST(UDTCanonicalArguments, DecoderRequiresCanonicalStructuralTypeFrames)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    const String uint64_arguments = fromBytes({1, 0, 1, 1, 1, 4});
    EXPECT_EQ(CanonicalTypeArguments::decode(parameters, uint64_arguments).encoded(), uint64_arguments);

    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 0x81, 0, 4})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 3, 0x16, 0x81, 0})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 2, 4, 0})), Exception);
    const String point_arguments = fromBytes({1, 0, 1, 1, 7, 0x2c, 5, 'P', 'o', 'i', 'n', 't'});
    EXPECT_EQ(CanonicalTypeArguments::decode(parameters, point_arguments).encoded(), point_arguments);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 6, 0x2c, 4, 'N', 'o', 'p', 'e'})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 1, 0x21})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 1, 0x24})), Exception);
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, fromBytes({1, 0, 1, 1, 1, 0x33})), Exception);

    std::vector<CanonicalTypeArgumentValue> nested_values;
    nested_values.push_back(CanonicalTypeArgumentValue::type(parseType("Array(Array(UInt64))")));
    const auto nested = CanonicalTypeArguments::validate(parameters, std::move(nested_values));
    CanonicalTypeArgumentLimits shallow;
    shallow.maximum_ast_depth = 2;
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, nested.encoded(), 64ULL << 10, 16ULL << 10, shallow), Exception);
}

TEST(UDTCanonicalArguments, DecoderEnforcesProspectiveStructuralBounds)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};

    const String wide_tuple = fromBytes({0x1f, 0x81, 0x80, 0x04});
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED, [&] { static_cast<void>(CanonicalTypeArguments::decode(parameters, wrapTypeFrame(wide_tuple))); });

    const String oversized_name = fromBytes({0x2c, 0x80, 0x80, 0x01});
    EXPECT_THROW(CanonicalTypeArguments::decode(parameters, wrapTypeFrame(oversized_name)), Exception);

    String aggregate_with_wide_field = fromBytes({0x25, 1, 3, 's', 'u', 'm', 1, 0x0d, 0x81, 0x20});
    CanonicalTypeArgumentLimits field_limits;
    field_limits.maximum_field_edges = 4'096;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        {
            static_cast<void>(CanonicalTypeArguments::decode(
                parameters, wrapTypeFrame(aggregate_with_wide_field), 64ULL << 10, 16ULL << 10, field_limits));
        });

    const auto timezone = makeSingleTypeArguments("DateTime('Europe/Berlin')");
    CanonicalTypeArgumentLimits string_limits;
    string_limits.maximum_owned_string_bytes = 20;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        { static_cast<void>(CanonicalTypeArguments::decode(parameters, timezone.encoded(), 64ULL << 10, 16ULL << 10, string_limits)); });

    const auto aggregate = makeSingleTypeArguments("AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64))");
    CanonicalTypeArgumentLimits depth_limits;
    depth_limits.maximum_field_depth = 1;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        { static_cast<void>(CanonicalTypeArguments::decode(parameters, aggregate.encoded(), 64ULL << 10, 16ULL << 10, depth_limits)); });
}

TEST(UDTCanonicalArguments, QBitUsesItsOwnProspectiveMaterializationBudget)
{
    const std::vector<Parameter> parameters{{.normalized_name = "T", .kind = ParameterKind::Type}};
    const auto wider_than_ast_budget = makeSingleTypeArguments("QBit(Float64, 520, 8)");
    EXPECT_EQ(CanonicalTypeArguments::decode(parameters, wider_than_ast_budget.encoded()), wider_than_ast_budget);

    CanonicalTypeArgumentLimits below_required;
    below_required.maximum_qbit_materialized_streams = 4'159;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        {
            static_cast<void>(
                CanonicalTypeArguments::decode(parameters, wider_than_ast_budget.encoded(), 64ULL << 10, 16ULL << 10, below_required));
        });
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&] { static_cast<void>(CanonicalTypeArgument::fromFactoryValidatedAST(parseType("QBit(Float64, 520, 8)"), below_required)); });

    const String too_many_groups = fromBytes({0x37, 0x07, 0x88, 0x40, 8});
    CanonicalTypeArgumentLimits small_materialization;
    small_materialization.maximum_qbit_materialized_streams = 8'191;
    expectDBError(
        ErrorCodes::LIMIT_EXCEEDED,
        [&]
        {
            static_cast<void>(CanonicalTypeArguments::decode(
                parameters, wrapTypeFrame(too_many_groups), 64ULL << 10, 16ULL << 10, small_materialization));
        });
}
}
