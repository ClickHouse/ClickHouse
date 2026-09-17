#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
}

namespace
{
using namespace DB;

using Classification = BuiltInDataTypeFamilyClassification;
using InputClass = BuiltInDataTypeCreatorInputClass;
using Match = BuiltInDataTypeFamilyMatch;
using Admission = BuiltInDataTypeAdmissionPath;

InputClass expectedInputClass(std::string_view canonical_creator_name)
{
    if (canonical_creator_name == "Enum" || canonical_creator_name == "Enum8" || canonical_creator_name == "Enum16")
        return InputClass::CanonicalizeGenericEnumArguments;
    return InputClass::ReadOnly;
}

String invertAsciiLetterCase(std::string_view value)
{
    String result(value);
    for (char & character : result)
    {
        if (character >= 'a' && character <= 'z')
            character = static_cast<char>(character - ('a' - 'A'));
        else if (character >= 'A' && character <= 'Z')
            character = static_cast<char>(character + ('a' - 'A'));
    }
    return result;
}

struct ObservedProductionClassifier
{
    mutable std::array<uint32_t, 4> calls_by_syntax{};

    static DataTypeFamilyClassification
    classify(const void * opaque_context, std::string_view family_name, DataTypeFamilySyntaxKind syntax_kind) noexcept
    {
        const auto & context = *static_cast<const ObservedProductionClassifier *>(opaque_context);
        ++context.calls_by_syntax[static_cast<size_t>(syntax_kind)];

        Classification classification;
        switch (syntax_kind)
        {
            case DataTypeFamilySyntaxKind::Generic: classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(family_name); break;
            case DataTypeFamilySyntaxKind::SpecializedEnum:
                classification = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(family_name);
                break;
            case DataTypeFamilySyntaxKind::SpecializedTuple:
                classification = BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(family_name);
                break;
            case DataTypeFamilySyntaxKind::QualifiedReference:
                classification = BuiltInDataTypeFamilyClassifier::classifyQualifiedReference();
                break;
        }

        return {
            .is_built_in = static_cast<bool>(classification),
            .is_qualified_reference = classification.admission == Admission::QualifiedUserType,
        };
    }

    DataTypeFamilyClassifier interface() const noexcept { return {.context = this, .callback = classify}; }

    uint32_t totalCalls() const noexcept
    {
        uint32_t result = 0;
        for (const auto calls : calls_by_syntax)
            result += calls;
        return result;
    }
};

struct ClassifiedParseResult
{
    ASTPtr ast;
    DataTypeFamilyClassificationSummary summary;
};

ClassifiedParseResult parseClassifiedDataType(const String & text, ObservedProductionClassifier & classifier)
{
    ClassifiedParseResult result;
    ParserDataTypeWithFamilyClassification parser(classifier.interface(), result.summary);
    result.ast = parseQuery(parser, text, "classified data type test", 0, 150, 0);
    return result;
}

struct CapturedError
{
    int code;
    String message;
};

template <typename Callback>
std::optional<CapturedError> captureException(Callback && callback)
{
    try
    {
        std::forward<Callback>(callback)();
        return std::nullopt;
    }
    catch (const Exception & exception)
    {
        return CapturedError{exception.code(), exception.message()};
    }
}

}

TEST(BuiltInDataTypeFamilyClassifier, FrozenInventoryExactlyMatchesLiveDataTypeFactory)
{
    auto & factory = DataTypeFactory::instance();
    auto names = factory.getAllRegisteredNames();
    std::sort(names.begin(), names.end());

    ASSERT_EQ(names.size(), BuiltInDataTypeFamilyClassifier::registeredFamilyCount());
    ASSERT_EQ(std::adjacent_find(names.begin(), names.end()), names.end());

    size_t maximum_name_size = 0;
    for (const auto & name : names)
    {
        SCOPED_TRACE(name);
        maximum_name_size = std::max(maximum_name_size, name.size());

        const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(name);
        ASSERT_TRUE(classification);
        ASSERT_NE(classification.family, nullptr);
        EXPECT_EQ(classification.match, Match::Exact);
        EXPECT_EQ(classification.admission, Admission::RegisteredGeneric);
        EXPECT_EQ(classification.family->registered_name, name);

        const bool is_alias = factory.isAlias(name);
        const String canonical_creator_name = is_alias ? factory.aliasTo(name) : name;
        EXPECT_EQ(classification.family->alias, is_alias);
        EXPECT_EQ(classification.family->canonical_creator_name, canonical_creator_name);
        EXPECT_EQ(classification.family->case_insensitive, factory.isCaseInsensitive(name));
        EXPECT_EQ(classification.family->input_class, expectedInputClass(canonical_creator_name));
        EXPECT_EQ(classification.input_class, expectedInputClass(canonical_creator_name));
        EXPECT_EQ(factory.getCreatorInputClass(name), classification.input_class);
        EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(name));
        EXPECT_TRUE(factory.collidesWithRegisteredFamilyOrAlias(name));

        const String inverted_case = invertAsciiLetterCase(name);
        EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(inverted_case));
        EXPECT_TRUE(factory.collidesWithRegisteredFamilyOrAlias(inverted_case));
        if (inverted_case != name && inverted_case != canonical_creator_name)
        {
            const auto folded = BuiltInDataTypeFamilyClassifier::classifyGeneric(inverted_case);
            if (factory.isCaseInsensitive(name))
            {
                ASSERT_TRUE(folded);
                ASSERT_NE(folded.family, nullptr);
                EXPECT_EQ(folded.match, Match::AsciiCaseInsensitive);
                EXPECT_EQ(folded.family->canonical_creator_name, canonical_creator_name);
                EXPECT_EQ(folded.input_class, expectedInputClass(canonical_creator_name));
            }
            else
            {
                EXPECT_FALSE(folded);
            }
        }
    }

    EXPECT_EQ(maximum_name_size, BuiltInDataTypeFamilyClassifier::maximumFamilyNameSize());
}

TEST(BuiltInDataTypeFamilyClassifier, GenericLookupPreservesExactFoldedAndMissSemantics)
{
    const auto exact = BuiltInDataTypeFamilyClassifier::classifyGeneric("UInt64");
    ASSERT_TRUE(exact);
    EXPECT_EQ(exact.match, Match::Exact);
    EXPECT_EQ(exact.family->registered_name, "UInt64");
    EXPECT_FALSE(exact.family->case_insensitive);

    const auto folded_creator = BuiltInDataTypeFamilyClassifier::classifyGeneric("dEcImAl");
    ASSERT_TRUE(folded_creator);
    EXPECT_EQ(folded_creator.match, Match::AsciiCaseInsensitive);
    EXPECT_EQ(folded_creator.family->registered_name, "Decimal");
    EXPECT_EQ(folded_creator.family->canonical_creator_name, "Decimal");
    EXPECT_FALSE(folded_creator.family->alias);

    const auto folded_alias = BuiltInDataTypeFamilyClassifier::classifyGeneric("vArChAr");
    ASSERT_TRUE(folded_alias);
    EXPECT_EQ(folded_alias.match, Match::AsciiCaseInsensitive);
    EXPECT_EQ(folded_alias.family->registered_name, "VARCHAR");
    EXPECT_EQ(folded_alias.family->canonical_creator_name, "String");
    EXPECT_TRUE(folded_alias.family->alias);

    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric("uInT64"));
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric("tUpLe"));
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric("BaselineUnknownType"));
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric(""));

    const String overlong(BuiltInDataTypeFamilyClassifier::maximumFamilyNameSize() + 1, 'x');
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric(overlong));
}

TEST(BuiltInDataTypeFamilyClassifier, UDTAdmissionCollisionIsCaseFoldedAcrossTheCompleteInventory)
{
    /// Ordinary factory classification remains case-sensitive for UInt64.
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric("uInT64"));
    EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("UInt64"));
    EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("uInT64"));

    /// Case-insensitive aliases collide under both their exact and mixed-case spellings.
    EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("VARCHAR"));
    EXPECT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("vArChAr"));

    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("ResolverUnknownType"));
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(""));
    const String overlong(BuiltInDataTypeFamilyClassifier::maximumFamilyNameSize() + 1, 'x');
    EXPECT_FALSE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(overlong));
}

TEST(BuiltInDataTypeFamilyClassifier, WarmedDirectLookupsDoNotAllocate)
{
    const String overlong(BuiltInDataTypeFamilyClassifier::maximumFamilyNameSize() + 1, 'x');

    /// Warm every lookup outcome before allocation denial so this assertion also
    /// rejects hidden first-use initialization in the production classifier.
    ASSERT_TRUE(BuiltInDataTypeFamilyClassifier::classifyGeneric("UInt64"));
    ASSERT_TRUE(BuiltInDataTypeFamilyClassifier::classifyGeneric("dEcImAl"));
    ASSERT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric("BaselineUnknownType"));
    ASSERT_FALSE(BuiltInDataTypeFamilyClassifier::classifyGeneric(overlong));
    ASSERT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("uInT64"));
    ASSERT_TRUE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("vArChAr"));
    ASSERT_FALSE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("ResolverUnknownType"));
    ASSERT_FALSE(BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(overlong));

    Classification exact;
    Classification folded;
    Classification unknown;
    Classification rejected_overlong;
    bool collision_sensitive_mixed_case = false;
    bool collision_alias_mixed_case = false;
    bool collision_unknown = true;
    bool collision_overlong = true;
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        exact = BuiltInDataTypeFamilyClassifier::classifyGeneric("UInt64");
        folded = BuiltInDataTypeFamilyClassifier::classifyGeneric("dEcImAl");
        unknown = BuiltInDataTypeFamilyClassifier::classifyGeneric("BaselineUnknownType");
        rejected_overlong = BuiltInDataTypeFamilyClassifier::classifyGeneric(overlong);
        collision_sensitive_mixed_case = BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("uInT64");
        collision_alias_mixed_case = BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("vArChAr");
        collision_unknown = BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias("ResolverUnknownType");
        collision_overlong = BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(overlong);
    }

    ASSERT_TRUE(exact);
    EXPECT_EQ(exact.match, Match::Exact);
    ASSERT_TRUE(folded);
    EXPECT_EQ(folded.match, Match::AsciiCaseInsensitive);
    EXPECT_FALSE(unknown);
    EXPECT_FALSE(rejected_overlong);
    EXPECT_TRUE(collision_sensitive_mixed_case);
    EXPECT_TRUE(collision_alias_mixed_case);
    EXPECT_FALSE(collision_unknown);
    EXPECT_FALSE(collision_overlong);
}

TEST(BuiltInDataTypeFamilyClassifier, SpecializedAdmissionsAndCreatorInputTraitsAreExplicit)
{
    for (const std::string_view name : {"Enum", "Enum8", "Enum16", "ENUM"})
    {
        SCOPED_TRACE(name);
        const auto generic = BuiltInDataTypeFamilyClassifier::classifyGeneric(name);
        ASSERT_TRUE(generic);
        EXPECT_EQ(generic.input_class, InputClass::CanonicalizeGenericEnumArguments);
    }

    const auto ordinary = BuiltInDataTypeFamilyClassifier::classifyGeneric("Array");
    ASSERT_TRUE(ordinary);
    EXPECT_EQ(ordinary.input_class, InputClass::ReadOnly);

    const auto enum_exact = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum("Enum16");
    ASSERT_TRUE(enum_exact);
    EXPECT_EQ(enum_exact.family->registered_name, "Enum16");
    EXPECT_EQ(enum_exact.match, Match::Exact);
    EXPECT_EQ(enum_exact.admission, Admission::SpecializedEnum);
    EXPECT_EQ(enum_exact.input_class, InputClass::SpecializedASTReadOnly);

    const auto enum_folded = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum("eNuM8");
    ASSERT_TRUE(enum_folded);
    EXPECT_EQ(enum_folded.family->registered_name, "Enum8");
    EXPECT_EQ(enum_folded.match, Match::AsciiCaseInsensitive);
    EXPECT_EQ(enum_folded.admission, Admission::SpecializedEnum);
    EXPECT_EQ(enum_folded.input_class, InputClass::SpecializedASTReadOnly);

    const auto enum_ast_kind = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum("ignored-by-specialized-ast");
    ASSERT_TRUE(enum_ast_kind);
    EXPECT_EQ(enum_ast_kind.family->registered_name, "Enum8");
    EXPECT_EQ(enum_ast_kind.match, Match::SpecializedASTKind);
    EXPECT_EQ(enum_ast_kind.input_class, InputClass::SpecializedASTReadOnly);

    const auto tuple_exact = BuiltInDataTypeFamilyClassifier::classifySpecializedTuple("Tuple");
    ASSERT_TRUE(tuple_exact);
    EXPECT_EQ(tuple_exact.family->registered_name, "Tuple");
    EXPECT_EQ(tuple_exact.match, Match::Exact);
    EXPECT_EQ(tuple_exact.admission, Admission::SpecializedTuple);
    EXPECT_EQ(tuple_exact.input_class, InputClass::SpecializedASTReadOnly);

    const auto tuple_ast_kind = BuiltInDataTypeFamilyClassifier::classifySpecializedTuple("ignored-by-specialized-ast");
    ASSERT_TRUE(tuple_ast_kind);
    EXPECT_EQ(tuple_ast_kind.family->registered_name, "Tuple");
    EXPECT_EQ(tuple_ast_kind.match, Match::SpecializedASTKind);
    EXPECT_EQ(tuple_ast_kind.admission, Admission::SpecializedTuple);

    const auto qualified = BuiltInDataTypeFamilyClassifier::classifyQualifiedReference();
    EXPECT_FALSE(qualified);
    EXPECT_EQ(qualified.family, nullptr);
    EXPECT_EQ(qualified.match, Match::QualifiedReference);
    EXPECT_EQ(qualified.admission, Admission::QualifiedUserType);

    DataTypeFamilyClassificationSummary summary;
    summary.add({.is_built_in = false, .is_qualified_reference = true});
    EXPECT_FALSE(summary.allFamiliesAreBuiltIn());
    EXPECT_TRUE(summary.hasQualifiedLogicalFamily());
}

TEST(BuiltInDataTypeFamilyClassifier, ClassifiedParserAggregatesNestedFamiliesDuringConstruction)
{
    ObservedProductionClassifier classifier;
    const auto parsed = parseClassifiedDataType("Array(Tuple(id UInt64, label Nullable(String), state Enum8('ok' = 1)))", classifier);

    ASSERT_NE(parsed.ast, nullptr);
    EXPECT_NE(parsed.ast->as<ASTDataType>(), nullptr);
    EXPECT_TRUE(parsed.summary.allFamiliesAreBuiltIn());
    EXPECT_FALSE(parsed.summary.hasQualifiedLogicalFamily());
    EXPECT_EQ(classifier.totalCalls(), 6);
    EXPECT_EQ(classifier.calls_by_syntax[static_cast<size_t>(DataTypeFamilySyntaxKind::Generic)], 4);
    EXPECT_EQ(classifier.calls_by_syntax[static_cast<size_t>(DataTypeFamilySyntaxKind::SpecializedEnum)], 1);
    EXPECT_EQ(classifier.calls_by_syntax[static_cast<size_t>(DataTypeFamilySyntaxKind::SpecializedTuple)], 1);

    ObservedProductionClassifier logical_classifier;
    const auto logical = parseClassifiedDataType("Array(Tuple(UInt64, BaselineDomain))", logical_classifier);
    EXPECT_FALSE(logical.summary.allFamiliesAreBuiltIn());
    EXPECT_FALSE(logical.summary.hasQualifiedLogicalFamily());
    EXPECT_EQ(logical_classifier.totalCalls(), 4);
}

TEST(BuiltInDataTypeFamilyClassifier, SpecializedTupleSpeculationDoesNotDoubleCountUnnamedElements)
{
    ObservedProductionClassifier classifier;
    const auto parsed = parseClassifiedDataType("Tuple(Array(UInt64), String)", classifier);

    ASSERT_NE(parsed.ast, nullptr);
    EXPECT_NE(parsed.ast->as<ASTTupleDataType>(), nullptr);
    EXPECT_TRUE(parsed.summary.allFamiliesAreBuiltIn());
    EXPECT_FALSE(parsed.summary.hasQualifiedLogicalFamily());
    EXPECT_EQ(classifier.totalCalls(), 4);
    EXPECT_EQ(classifier.calls_by_syntax[static_cast<size_t>(DataTypeFamilySyntaxKind::Generic)], 3);
    EXPECT_EQ(classifier.calls_by_syntax[static_cast<size_t>(DataTypeFamilySyntaxKind::SpecializedTuple)], 1);
}

TEST(BuiltInDataTypeFamilyClassifier, OptInFactoryPathPreservesPhysicalTypesAndErrors)
{
    auto & factory = DataTypeFactory::instance();
    const std::array valid_constructions{
        String{"UInt64"},
        String{"vArChAr(255)"},
        String{"Array(Tuple(id UInt64, label Nullable(String)))"},
        String{"Tuple(Array(UInt64), String)"},
        String{"Enum16('ready' = 1, 'failed' = 2)"},
    };

    for (const auto & construction : valid_constructions)
    {
        SCOPED_TRACE(construction);
        const auto direct = factory.get(construction);
        const auto classified = factory.getWithFamilyClassification(construction);
        ASSERT_NE(direct, nullptr);
        ASSERT_NE(classified, nullptr);
        EXPECT_TRUE(classified->equals(*direct));
        EXPECT_EQ(classified->getName(), direct->getName());
    }

    const std::array invalid_constructions{
        String{"BaselineUnknownType"},
        String{"Array"},
        String{"Enum8('too_wide' = 128)"},
        String{"Array(UInt64"},
    };

    for (const auto & construction : invalid_constructions)
    {
        SCOPED_TRACE(construction);
        const auto direct_error = captureException([&] { static_cast<void>(factory.get(construction)); });
        const auto classified_error = captureException([&] { static_cast<void>(factory.getWithFamilyClassification(construction)); });
        ASSERT_TRUE(direct_error.has_value());
        ASSERT_TRUE(classified_error.has_value());
        EXPECT_EQ(classified_error->code, direct_error->code);
        EXPECT_EQ(classified_error->message, direct_error->message);
    }
}

TEST(BuiltInDataTypeFamilyClassifier, QualifiedLogicalFactoryCandidatesStopAtUDTBoundary)
{
    auto & factory = DataTypeFactory::instance();
    for (const String construction : {"app.UserId", "Array(app.UserId)", "Tuple(UInt64, ids.Raw(16))"})
    {
        SCOPED_TRACE(construction);
        const auto error = captureException([&] { static_cast<void>(factory.getWithFamilyClassification(construction)); });
        ASSERT_TRUE(error.has_value());
        EXPECT_EQ(error->code, DB::ErrorCodes::SUPPORT_IS_DISABLED);
    }
}

TEST(BuiltInDataTypeFamilyClassifier, SyntaxOnlyQualifiedBoundaryDoesNotConstructBuiltInTypes)
{
    auto & factory = DataTypeFactory::instance();
    EXPECT_NO_THROW(factory.rejectQualifiedUDTSyntax("Enum8('dotted.value' = 1)"));
    EXPECT_NO_THROW(factory.rejectQualifiedUDTSyntax("Array(Tuple(UInt64, String))"));

    const auto logical_error = captureException([&] { factory.rejectQualifiedUDTSyntax("Array(app.UserId)"); });
    ASSERT_TRUE(logical_error.has_value());
    EXPECT_EQ(logical_error->code, DB::ErrorCodes::SUPPORT_IS_DISABLED);

    const auto collision_error = captureException([&] { factory.rejectQualifiedUDTSyntax("app.vArChAr"); });
    ASSERT_TRUE(collision_error.has_value());
    EXPECT_EQ(collision_error->code, DB::ErrorCodes::BAD_ARGUMENTS);
}
