#include <DataTypes/UDT/TemplateCheckerCertificateEncoding.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace
{

namespace Certificate = DB::UDT::CheckerProof;

using Certificate::AggregateFunctionNullsAction;
using Certificate::Byte;
using Certificate::CanonicalFieldKind;
using Certificate::CanonicalIRChildView;
using Certificate::CanonicalIREnumEntryView;
using Certificate::LegacyCanonicalIRNodeKind;
using Certificate::CanonicalIRNodeKind;
using Certificate::LegacyCanonicalIRNodeView;
using Certificate::CanonicalIRNodeView;
using Certificate::LegacyCanonicalTemplateIRView;
using Certificate::CanonicalTemplateIRView;
using Certificate::EncodingError;
using Certificate::EncodingLimits;
using Certificate::SpecializedEnumWidth;

std::string toHex(std::span<const Byte> bytes)
{
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const Byte byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

std::vector<Byte> encodeCanonicalIRBytes(const CanonicalTemplateIRView & ir, EncodingLimits limits = {})
{
    const auto size = Certificate::encodeCanonicalTemplateIR(ir, {}, limits);
    std::vector<Byte> result(size);
    EXPECT_EQ(Certificate::encodeCanonicalTemplateIR(ir, result, limits), size);
    return result;
}

template <typename Function>
void expectEncodingError(EncodingError::Code code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected a canonical-encoding error";
    }
    catch (const EncodingError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

constexpr CanonicalIRChildView child(std::uint32_t reference, std::string_view label = {})
{
    CanonicalIRChildView result;
    result.reference = reference;
    result.label = label;
    return result;
}

constexpr CanonicalIREnumEntryView enumEntry(std::string_view name, std::int64_t value)
{
    CanonicalIREnumEntryView result;
    result.name = name;
    result.value = value;
    return result;
}

class CanonicalIRNodeBuilder
{
public:
    constexpr explicit CanonicalIRNodeBuilder(CanonicalIRNodeKind kind) { value.kind = kind; }

    constexpr CanonicalIRNodeBuilder withAtom(std::string_view atom) const
    {
        auto result = *this;
        result.value.atom = atom;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withText(std::string_view text) const
    {
        auto result = *this;
        result.value.text = text;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withParameter(std::uint16_t parameter) const
    {
        auto result = *this;
        result.value.parameter = parameter;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withDecrement(std::uint64_t decrement) const
    {
        auto result = *this;
        result.value.decrement = decrement;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withUnsignedLiteral(std::uint64_t literal) const
    {
        auto result = *this;
        result.value.unsigned_literal = literal;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withSignedLiteral(std::int64_t literal) const
    {
        auto result = *this;
        result.value.signed_literal = literal;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withBooleanLiteral(bool literal) const
    {
        auto result = *this;
        result.value.boolean_literal = literal;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withDependencyOrdinal(std::uint16_t ordinal) const
    {
        auto result = *this;
        result.value.dependency_ordinal = ordinal;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withChildren(std::span<const CanonicalIRChildView> children) const
    {
        auto result = *this;
        result.value.children = children;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withSpecializedEnum(SpecializedEnumWidth width, std::span<const CanonicalIREnumEntryView> entries) const
    {
        auto result = *this;
        result.value.specialized_enum_width = width;
        result.value.enum_entries = entries;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withField(CanonicalFieldKind kind, std::span<const Byte> payload = {}, std::string_view name = {}) const
    {
        auto result = *this;
        result.value.field_kind = kind;
        result.value.field_payload = payload;
        result.value.field_name = name;
        return result;
    }

    constexpr CanonicalIRNodeBuilder withNullsAction(AggregateFunctionNullsAction action) const
    {
        auto result = *this;
        result.value.aggregate_nulls_action = action;
        return result;
    }

    constexpr operator CanonicalIRNodeView() const { return value; }

private:
    CanonicalIRNodeView value{};
};

constexpr CanonicalIRNodeBuilder canonicalNode(CanonicalIRNodeKind kind)
{
    return CanonicalIRNodeBuilder(kind);
}

constexpr CanonicalTemplateIRView
canonicalIR(std::span<const CanonicalIRNodeView> nodes, std::uint16_t formal_count = 0, std::uint16_t dependency_count = 0)
{
    CanonicalTemplateIRView result;
    result.formal_count = formal_count;
    result.direct_dependency_count = dependency_count;
    result.nodes = nodes;
    return result;
}

std::vector<Byte> encodeRepresentativeCanonicalIR()
{
    constexpr std::array<CanonicalIRChildView, 9> root_children{
        child(1, "signed"),
        child(2),
        child(3),
        child(4),
        child(5),
        child(6),
        child(7),
        child(8),
        child(9),
    };
    constexpr std::array<CanonicalIRChildView, 2> type_if_children{
        child(10),
        child(11),
    };
    constexpr std::array<CanonicalIRChildView, 2> external_actuals{
        child(0),
        child(1),
    };
    constexpr std::array<CanonicalIREnumEntryView, 3> enum_entries{
        enumEntry("", std::numeric_limits<std::int16_t>::min()),
        enumEntry("ready", -2),
        enumEntry("done", 300),
    };
    constexpr char binary_literal[] = {'A', '\0', 'B'};
    const std::array<CanonicalIRNodeView, 12> nodes{
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Tuple").withChildren(root_children),
        canonicalNode(CanonicalIRNodeKind::SignedLiteral).withSignedLiteral(-42),
        canonicalNode(CanonicalIRNodeKind::StringLiteral).withText(std::string_view(binary_literal, sizeof(binary_literal))),
        canonicalNode(CanonicalIRNodeKind::Identifier).withText("field"),
        canonicalNode(CanonicalIRNodeKind::SpecializedEnum).withSpecializedEnum(SpecializedEnumWidth::Enum16, enum_entries),
        canonicalNode(CanonicalIRNodeKind::UnsignedLiteral).withUnsignedLiteral(300),
        canonicalNode(CanonicalIRNodeKind::BooleanLiteral).withBooleanLiteral(true),
        canonicalNode(CanonicalIRNodeKind::TypeFormal).withParameter(0),
        canonicalNode(CanonicalIRNodeKind::ValueFormal).withParameter(1),
        canonicalNode(CanonicalIRNodeKind::TypeIfZero).withParameter(1).withChildren(type_if_children),
        canonicalNode(CanonicalIRNodeKind::SelfCall).withParameter(1).withDecrement(1),
        canonicalNode(CanonicalIRNodeKind::ExternalCall).withDependencyOrdinal(0).withChildren(external_actuals),
    };
    return encodeCanonicalIRBytes(canonicalIR(nodes, 2, 1));
}

std::vector<Byte> encodeTypedParserSurfaceCanonicalIR()
{
    constexpr std::array root_children{child(1), child(2), child(3)};
    constexpr std::array aggregate_type_children{child(4), child(5), child(6)};
    constexpr std::array dynamic_children{child(7)};
    constexpr std::array object_children{child(8), child(9), child(10), child(11)};
    constexpr std::array aggregate_parameters{child(12)};
    constexpr std::array array_type_children{child(13)};
    constexpr std::array dynamic_setting_value{child(14)};
    constexpr std::array object_setting_value{child(15)};
    constexpr std::array typed_path_type{child(16)};
    constexpr std::array array_values{child(17), child(18), child(19)};
    constexpr std::array<Byte, 8> unsigned_one{1, 0, 0, 0, 0, 0, 0, 0};
    constexpr std::array<Byte, 8> signed_minus_one{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    constexpr char binary_regexp[] = {'^', 't', 'm', 'p', '\0', '$'};

    const std::array<CanonicalIRNodeView, 20> nodes{
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Tuple").withChildren(root_children),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("AggregateFunction").withChildren(aggregate_type_children),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Dynamic").withChildren(dynamic_children),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("JSON").withChildren(object_children),
        canonicalNode(CanonicalIRNodeKind::UnsignedLiteral).withUnsignedLiteral(7),
        canonicalNode(CanonicalIRNodeKind::AggregateFunction)
            .withText("sumMapFiltered")
            .withNullsAction(AggregateFunctionNullsAction::RespectNulls)
            .withChildren(aggregate_parameters),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Array").withChildren(array_type_children),
        canonicalNode(CanonicalIRNodeKind::DynamicSetting).withText("max_types").withChildren(dynamic_setting_value),
        canonicalNode(CanonicalIRNodeKind::ObjectSetting).withText("max_dynamic_paths").withChildren(object_setting_value),
        canonicalNode(CanonicalIRNodeKind::ObjectTypedPath).withText("payload.value").withChildren(typed_path_type),
        canonicalNode(CanonicalIRNodeKind::ObjectSkipPath).withText("private.path"),
        canonicalNode(CanonicalIRNodeKind::ObjectSkipRegexp).withText(std::string_view(binary_regexp, sizeof(binary_regexp))),
        canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Array).withChildren(array_values),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("UInt64"),
        canonicalNode(CanonicalIRNodeKind::UnsignedLiteral).withUnsignedLiteral(5),
        canonicalNode(CanonicalIRNodeKind::UnsignedLiteral).withUnsignedLiteral(9),
        canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("String"),
        canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::UInt64, unsigned_one),
        canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Int64, signed_minus_one),
        canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null),
    };
    return encodeCanonicalIRBytes(canonicalIR(nodes));
}

constexpr std::size_t canonicalNodeCountOffset()
{
    return Certificate::canonical_template_ir_domain.size() + 1 + 2 + 2 + 2;
}

TEST(UDTTemplateCertificateEncoding, LegacyWireContractAndGoldenRemainUnchanged)
{
    static_assert(Certificate::legacy_canonical_ir_version == 2);
    static_assert(static_cast<Byte>(LegacyCanonicalIRNodeKind::BuiltIn) == 1);
    static_assert(static_cast<Byte>(LegacyCanonicalIRNodeKind::ExternalCall) == 8);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::BuiltIn) == 1);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::ExternalCall) == 8);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::SignedLiteral) == 9);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::SpecializedEnum) == 12);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::FieldValue) == 13);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::AggregateFunction) == 14);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::DynamicSetting) == 15);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::ObjectSetting) == 16);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::ObjectTypedPath) == 17);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::ObjectSkipPath) == 18);
    static_assert(static_cast<Byte>(CanonicalIRNodeKind::ObjectSkipRegexp) == 19);
    static_assert(static_cast<Byte>(CanonicalFieldKind::Null) == 1);
    static_assert(static_cast<Byte>(CanonicalFieldKind::IPv6) == 17);
    static_assert(static_cast<Byte>(CanonicalFieldKind::NegativeInfinity) == 18);
    static_assert(static_cast<Byte>(CanonicalFieldKind::AggregateFunctionState) == 24);

    constexpr auto type_formal = []
    {
        LegacyCanonicalIRNodeView result;
        result.kind = LegacyCanonicalIRNodeKind::TypeFormal;
        result.parameter = 0;
        return result;
    }();
    constexpr std::array<LegacyCanonicalIRNodeView, 1> nodes{type_formal};
    LegacyCanonicalTemplateIRView ir;
    ir.formal_count = 1;
    ir.nodes = nodes;
    const auto size = Certificate::encodeLegacyCanonicalTemplateIR(ir);
    std::vector<Byte> bytes(size);
    ASSERT_EQ(Certificate::encodeLegacyCanonicalTemplateIR(ir, bytes), size);
    EXPECT_EQ(toHex(bytes), "02000100000001020000");
}

TEST(UDTTemplateCertificateEncoding, CanonicalRepresentativeGoldenIsStableAndStrictlyDecodable)
{
    const auto bytes = encodeRepresentativeCanonicalIR();
    EXPECT_EQ(
        toHex(bytes),
        "436c69636b486f757365205544542063616e6f6e6963616c2074656d706c617465204952205633000300020001000c01055475706c6509067369676e6564010002"
        "00"
        "0300040005000600070008000909530a034100420b056669656c640c020300ffff030572656164790304646f6e65d80404ac0205010200000301000601000a0b07"
        "0100010800000200000100");
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(bytes));
}

TEST(UDTTemplateCertificateEncoding, CanonicalTypedParserSurfaceGoldenIsStableAndStrictlyDecodable)
{
    const auto bytes = encodeTypedParserSurfaceCanonicalIR();
    EXPECT_EQ(
        toHex(bytes),
        "436c69636b486f757365205544542063616e6f6e6963616c2074656d706c617465204952205633000300000000001401055475706c650300010002000301114167"
        "6772656761746546756e6374696f6e03000400050006010744796e616d696301000701044a534f4e0400080009000a000b04070e0e73756d4d617046696c7465"
        "72656401010c0105417272617901000d0f096d61785f74797065730e10116d61785f64796e616d69635f70617468730f110d7061796c6f61642e76616c756510"
        "120c707269766174652e7061746813065e746d7000240d1403111213010655496e74363400040504090106537472696e67000d0201000000000000000d03ffff"
        "ffffffffffff0d01");
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(bytes));
}

TEST(UDTTemplateCertificateEncoding, CanonicalEncoderIsAnExactTwoPassContract)
{
    const std::array<CanonicalIRNodeView, 1> nodes{
        canonicalNode(CanonicalIRNodeKind::SignedLiteral).withSignedLiteral(std::numeric_limits<std::int64_t>::min()),
    };
    const CanonicalTemplateIRView ir = canonicalIR(nodes);
    const auto measured = Certificate::encodeCanonicalTemplateIR(ir);
    ASSERT_GT(measured, 0);

    std::vector<Byte> exact(measured);
    EXPECT_EQ(Certificate::encodeCanonicalTemplateIR(ir, exact), measured);
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(exact));

    std::vector<Byte> short_output(measured - 1);
    expectEncodingError(EncodingError::Code::OutputSizeMismatch, [&] { Certificate::encodeCanonicalTemplateIR(ir, short_output); });
    std::vector<Byte> long_output(measured + 1);
    expectEncodingError(EncodingError::Code::OutputSizeMismatch, [&] { Certificate::encodeCanonicalTemplateIR(ir, long_output); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalFieldInventoryHasExactValuePreservingPayloadContracts)
{
    constexpr std::array<Byte, 1> one{1};
    constexpr std::array<Byte, 4> four{0, 1, 2, 3};
    constexpr std::array<Byte, 8> eight{0, 1, 2, 3, 4, 5, 6, 7};
    constexpr std::array<Byte, 12> twelve{};
    constexpr std::array<Byte, 16> sixteen{};
    constexpr std::array<Byte, 20> twenty{};
    constexpr std::array<Byte, 32> thirty_two{};
    constexpr std::array<Byte, 36> thirty_six{};
    constexpr std::array<Byte, 3> binary_string{'x', 0, 'y'};

    const auto accepts = [&](CanonicalFieldKind kind, std::span<const Byte> payload = {}, std::string_view name = {})
    {
        const std::array<CanonicalIRNodeView, 1> nodes{
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(kind, payload, name),
        };
        const auto bytes = encodeCanonicalIRBytes(canonicalIR(nodes));
        EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(bytes));
    };

    accepts(CanonicalFieldKind::Null);
    accepts(CanonicalFieldKind::NegativeInfinity);
    accepts(CanonicalFieldKind::PositiveInfinity);
    accepts(CanonicalFieldKind::Bool, one);
    accepts(CanonicalFieldKind::IPv4, four);
    accepts(CanonicalFieldKind::UInt64, eight);
    accepts(CanonicalFieldKind::Int64, eight);
    accepts(CanonicalFieldKind::Float64, eight); /// Exact IEEE bits, including noncanonical NaN payloads.
    accepts(CanonicalFieldKind::Decimal32, eight);
    accepts(CanonicalFieldKind::Decimal64, twelve);
    accepts(CanonicalFieldKind::UInt128, sixteen);
    accepts(CanonicalFieldKind::Int128, sixteen);
    accepts(CanonicalFieldKind::UUID, sixteen);
    accepts(CanonicalFieldKind::IPv6, sixteen);
    accepts(CanonicalFieldKind::Decimal128, twenty);
    accepts(CanonicalFieldKind::UInt256, thirty_two);
    accepts(CanonicalFieldKind::Int256, thirty_two);
    accepts(CanonicalFieldKind::Decimal256, thirty_six);
    accepts(CanonicalFieldKind::String, binary_string);
    accepts(CanonicalFieldKind::AggregateFunctionState, binary_string, "AggregateFunction(sum, UInt64)");

    constexpr std::array<Byte, 8> positive_zero{};
    std::array<Byte, 8> negative_zero{};
    negative_zero.back() = 0x80;
    const std::array positive_zero_nodes{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Float64, positive_zero)),
    };
    const std::array negative_zero_nodes{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Float64, negative_zero)),
    };
    EXPECT_NE(encodeCanonicalIRBytes(canonicalIR(positive_zero_nodes)), encodeCanonicalIRBytes(canonicalIR(negative_zero_nodes)));

    auto bad_bool = one;
    bad_bool[0] = 2;
    const std::array bad_bool_nodes{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Bool, bad_bool)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(bad_bool_nodes)); });

    const std::array wrong_width_nodes{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::UInt64, four)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(wrong_width_nodes)); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalCompositeFieldsAreStructuralAndCanonical)
{
    constexpr std::array<CanonicalIRChildView, 2> array_children{child(1), child(2)};
    constexpr std::array<Byte, 8> value{};
    const std::array valid_array{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Array).withChildren(array_children)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::UInt64, value)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
    };
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(encodeCanonicalIRBytes(canonicalIR(valid_array))));

    constexpr std::array<CanonicalIRChildView, 1> odd_map_children{child(1)};
    const std::array odd_map{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Map).withChildren(odd_map_children)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(odd_map)); });

    constexpr std::array unordered_object_children{child(1, "b"), child(2, "a")};
    const std::array unordered_object{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Object).withChildren(unordered_object_children)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(unordered_object)); });

    constexpr std::array duplicate_object_children{child(1, "a"), child(2, "a")};
    const std::array duplicate_object{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Object).withChildren(duplicate_object_children)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(duplicate_object)); });

    constexpr std::array aggregate_parameter{child(1)};
    const std::array wrong_aggregate_child{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::AggregateFunction).withText("sum").withChildren(aggregate_parameter)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("UInt64")),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(wrong_aggregate_child)); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalRejectsEveryInactiveUnionField)
{
    CanonicalIRNodeView node = canonicalNode(CanonicalIRNodeKind::SignedLiteral).withSignedLiteral(-1);
    const auto rejects = [&]
    {
        const std::array nodes{node};
        expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::encodeCanonicalTemplateIR(canonicalIR(nodes)); });
    };

    node.atom = "opaque SQL";
    rejects();
    node.atom = {};
    node.unsigned_literal = 1;
    rejects();
    node.unsigned_literal = 0;
    node.text = "opaque SQL";
    rejects();
    node.text = {};
    node.children = std::span<const CanonicalIRChildView>{};
    node.specialized_enum_width = SpecializedEnumWidth::Enum8;
    rejects();

    node = canonicalNode(CanonicalIRNodeKind::StringLiteral).withText("value").withSignedLiteral(1);
    rejects();
    node = canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Array").withText("hidden");
    rejects();
}

TEST(UDTTemplateCertificateEncoding, CanonicalRejectsBadDiscoveryOrderAndUnreachableNodes)
{
    constexpr std::array<CanonicalIRChildView, 1> skips_first{child(2)};
    const std::array nodes_with_gap{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Array").withChildren(skips_first)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::UnsignedLiteral)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::UnsignedLiteral)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::encodeCanonicalTemplateIR(canonicalIR(nodes_with_gap)); });

    constexpr std::array<CanonicalIRChildView, 1> backward{child(0)};
    const std::array nodes_with_back_edge{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Array").withChildren(backward)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::encodeCanonicalTemplateIR(canonicalIR(nodes_with_back_edge)); });

    const std::array unreachable{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::UnsignedLiteral)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::UnsignedLiteral)),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::encodeCanonicalTemplateIR(canonicalIR(unreachable)); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalSpecializedEnumIsBoundedAndCanonicallyOrdered)
{
    constexpr std::array good_entries{
        enumEntry("", -128),
        enumEntry("ok", 127),
    };
    CanonicalIRNodeView enum_node
        = canonicalNode(CanonicalIRNodeKind::SpecializedEnum).withSpecializedEnum(SpecializedEnumWidth::Enum8, good_entries);
    const auto encode_enum = [&]
    {
        const std::array nodes{enum_node};
        return encodeCanonicalIRBytes(canonicalIR(nodes));
    };
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(encode_enum()));

    constexpr std::array out_of_order{
        enumEntry("first", 1),
        enumEntry("second", 0),
    };
    enum_node.enum_entries = out_of_order;
    expectEncodingError(EncodingError::Code::InvalidValue, encode_enum);

    constexpr std::array out_of_range{enumEntry("too-large", 128)};
    enum_node.enum_entries = out_of_range;
    expectEncodingError(EncodingError::Code::InvalidValue, encode_enum);

    enum_node.enum_entries = {};
    expectEncodingError(EncodingError::Code::InvalidValue, encode_enum);
    enum_node.enum_entries = good_entries;
    enum_node.specialized_enum_width = SpecializedEnumWidth::None;
    expectEncodingError(EncodingError::Code::InvalidValue, encode_enum);

    enum_node.specialized_enum_width = SpecializedEnumWidth::Enum8;
    EncodingLimits limits;
    limits.maximum_ir_enum_entries = 1;
    const std::array nodes{enum_node};
    expectEncodingError(EncodingError::Code::LimitExceeded, [&] { Certificate::encodeCanonicalTemplateIR(canonicalIR(nodes), {}, limits); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalStringsAreBinarySafeButIdentifiersAreNormalizedTokens)
{
    constexpr char binary_text[] = {'x', '\0', 'y'};
    CanonicalIRNodeView node = canonicalNode(CanonicalIRNodeKind::StringLiteral).withText(std::string_view(binary_text, sizeof(binary_text)));
    const auto encode_node = [&](EncodingLimits limits = {})
    {
        const std::array nodes{node};
        return encodeCanonicalIRBytes(canonicalIR(nodes), limits);
    };
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(encode_node()));

    node.kind = CanonicalIRNodeKind::Identifier;
    expectEncodingError(EncodingError::Code::InvalidValue, encode_node);
    node.text = {};
    expectEncodingError(EncodingError::Code::InvalidValue, encode_node);
    node.text = "field";
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(encode_node()));

    EncodingLimits identifier_limits;
    identifier_limits.maximum_ir_identifier_bytes = 4;
    expectEncodingError(EncodingError::Code::LimitExceeded, [&] { encode_node(identifier_limits); });
    node.kind = CanonicalIRNodeKind::StringLiteral;
    EncodingLimits literal_limits;
    literal_limits.maximum_ir_literal_bytes = 4;
    expectEncodingError(EncodingError::Code::LimitExceeded, [&] { encode_node(literal_limits); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalRawValidatorRejectsAlternateAndOpaqueEncodings)
{
    const std::array<CanonicalIRNodeView, 1> signed_nodes{
        canonicalNode(CanonicalIRNodeKind::SignedLiteral),
    };
    const auto canonical = encodeCanonicalIRBytes(canonicalIR(signed_nodes));
    const std::size_t count_offset = canonicalNodeCountOffset();
    const std::size_t tag_offset = count_offset + 1;
    const std::size_t payload_offset = tag_offset + 1;
    ASSERT_LT(payload_offset, canonical.size());

    auto nonminimal_count = canonical;
    nonminimal_count[count_offset] = 0x81;
    nonminimal_count.insert(nonminimal_count.begin() + count_offset + 1, 0x00);
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(nonminimal_count); });

    auto nonminimal_signed = canonical;
    nonminimal_signed[payload_offset] = 0x80;
    nonminimal_signed.insert(nonminimal_signed.begin() + payload_offset + 1, 0x00);
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(nonminimal_signed); });

    auto opaque_sql_tag = canonical;
    opaque_sql_tag[tag_offset] = 20;
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(opaque_sql_tag); });

    auto wrong_version = canonical;
    wrong_version[Certificate::canonical_template_ir_domain.size() + 1] = 4;
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(wrong_version); });

    auto trailing = canonical;
    trailing.push_back(0);
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(trailing); });

    auto truncated = canonical;
    truncated.pop_back();
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(truncated); });

    EncodingLimits too_small;
    too_small.maximum_template_ir_bytes = canonical.size() - 1;
    expectEncodingError(
        EncodingError::Code::LimitExceeded, [&] { Certificate::validateEncodedCanonicalTemplateIR(canonical, too_small); });

    const std::array<CanonicalIRNodeView, 1> boolean_nodes{
        canonicalNode(CanonicalIRNodeKind::BooleanLiteral),
    };
    auto noncanonical_boolean = encodeCanonicalIRBytes(canonicalIR(boolean_nodes));
    noncanonical_boolean[payload_offset] = 2;
    expectEncodingError(
        EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(noncanonical_boolean); });

    constexpr std::array<Byte, 1> true_payload{1};
    const std::array<CanonicalIRNodeView, 1> field_nodes{
        canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Bool, true_payload),
    };
    const auto canonical_field = encodeCanonicalIRBytes(canonicalIR(field_nodes));
    const std::size_t field_kind_offset = tag_offset + 1;
    const std::size_t field_payload_offset = field_kind_offset + 1;
    ASSERT_LT(field_payload_offset, canonical_field.size());

    auto unknown_field_kind = canonical_field;
    unknown_field_kind[field_kind_offset] = 25;
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(unknown_field_kind); });

    auto noncanonical_field_bool = canonical_field;
    noncanonical_field_bool[field_payload_offset] = 2;
    expectEncodingError(
        EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(noncanonical_field_bool); });

    auto truncated_field = canonical_field;
    truncated_field.pop_back();
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(truncated_field); });
}

TEST(UDTTemplateCertificateEncoding, CanonicalRawValidationIsWireShapeOnlyAndNeverSemanticAdmission)
{
    constexpr std::array aggregate_parameter{child(1)};
    const std::array aggregate_nodes{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::AggregateFunction).withText("sum").withChildren(aggregate_parameter)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::FieldValue).withField(CanonicalFieldKind::Null)),
    };
    auto wrong_target_kind = encodeCanonicalIRBytes(canonicalIR(aggregate_nodes));
    /// The FieldValue tag is followed by a Null-kind byte (1). Reinterpreting
    /// those same two bytes as UnsignedLiteral(1) preserves a canonical wire
    /// shape, but violates AggregateFunction's semantic child-kind contract.
    const std::size_t second_node_tag = canonicalNodeCountOffset() + 9;
    ASSERT_LT(second_node_tag + 1, wrong_target_kind.size());
    ASSERT_EQ(wrong_target_kind[second_node_tag], static_cast<Byte>(CanonicalIRNodeKind::FieldValue));
    ASSERT_EQ(wrong_target_kind[second_node_tag + 1], static_cast<Byte>(CanonicalFieldKind::Null));
    wrong_target_kind[second_node_tag] = static_cast<Byte>(CanonicalIRNodeKind::UnsignedLiteral);
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(wrong_target_kind));

    constexpr std::array duplicate_labels{
        enumEntry("same", 0),
        enumEntry("same", 1),
    };
    const std::array duplicate_label_nodes{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::SpecializedEnum).withSpecializedEnum(SpecializedEnumWidth::Enum8, duplicate_labels)),
    };
    const auto duplicate_label_bytes = encodeCanonicalIRBytes(canonicalIR(duplicate_label_nodes));
    EXPECT_NO_THROW(Certificate::validateEncodedCanonicalTemplateIR(duplicate_label_bytes));
}

TEST(UDTTemplateCertificateEncoding, CanonicalBuiltInAtomsAndFieldLabelsCannotHideNUL)
{
    constexpr char atom_with_nul[] = {'A', '\0', 'B'};
    const std::array bad_atom_nodes{
        static_cast<CanonicalIRNodeView>(
            canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom(std::string_view(atom_with_nul, sizeof(atom_with_nul)))),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(bad_atom_nodes)); });

    static constexpr char label_with_nul[] = {'x', '\0', 'y'};
    constexpr std::array bad_label_children{child(1, std::string_view(label_with_nul, sizeof(label_with_nul)))};
    const std::array bad_label_nodes{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("Tuple").withChildren(bad_label_children)),
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("UInt64")),
    };
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { encodeCanonicalIRBytes(canonicalIR(bad_label_nodes)); });

    const std::array good_atom_nodes{
        static_cast<CanonicalIRNodeView>(canonicalNode(CanonicalIRNodeKind::BuiltIn).withAtom("X")),
    };
    auto raw_bad_atom = encodeCanonicalIRBytes(canonicalIR(good_atom_nodes));
    const std::size_t atom_payload = canonicalNodeCountOffset() + 3;
    ASSERT_LT(atom_payload, raw_bad_atom.size());
    raw_bad_atom[atom_payload] = 0;
    expectEncodingError(EncodingError::Code::InvalidValue, [&] { Certificate::validateEncodedCanonicalTemplateIR(raw_bad_atom); });
}

}
