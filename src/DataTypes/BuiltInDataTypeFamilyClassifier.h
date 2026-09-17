#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace DB
{

/// Describes whether a registered creator may change the generic type AST it receives.
enum class BuiltInDataTypeCreatorInputClass : std::uint8_t
{
    ReadOnly,
    CanonicalizeGenericEnumArguments,
    SpecializedASTReadOnly,
};

enum class BuiltInDataTypeFamilyMatch : std::uint8_t
{
    None,
    Exact,
    AsciiCaseInsensitive,
    SpecializedASTKind,
    QualifiedReference,
};

enum class BuiltInDataTypeAdmissionPath : std::uint8_t
{
    None,
    RegisteredGeneric,
    SpecializedEnum,
    SpecializedTuple,
    QualifiedUserType,
};

/// Immutable metadata for one spelling registered by DataTypeFactory.
struct BuiltInDataTypeFamilyInfo
{
    std::string_view registered_name;
    std::string_view canonical_creator_name;
    /// Effective name-resolution behavior after aliases are installed.
    bool case_insensitive = false;
    /// Case policy declared at this spelling's own registration call.
    bool registration_case_insensitive = false;
    bool alias = false;
    BuiltInDataTypeCreatorInputClass input_class = BuiltInDataTypeCreatorInputClass::ReadOnly;
};

struct BuiltInDataTypeFamilyClassification
{
    const BuiltInDataTypeFamilyInfo * family = nullptr;
    BuiltInDataTypeFamilyMatch match = BuiltInDataTypeFamilyMatch::None;
    BuiltInDataTypeAdmissionPath admission = BuiltInDataTypeAdmissionPath::None;
    BuiltInDataTypeCreatorInputClass input_class = BuiltInDataTypeCreatorInputClass::ReadOnly;

    explicit operator bool() const noexcept { return family != nullptr; }
};

/**
 * Stateless classifier for the frozen built-in DataTypeFactory inventory.
 *
 * Lookup reads compile-time open-addressed tables. It does not allocate, copy,
 * lowercase, lock, or run first-use initialization. Generic misses longer than
 * every registered spelling are rejected before hashing.
 */
class BuiltInDataTypeFamilyClassifier final
{
public:
    [[nodiscard]] static BuiltInDataTypeFamilyClassification classifyGeneric(std::string_view family_name) noexcept;
    /// UDT names reserve the ASCII-case-folded spelling of every registered
    /// family and alias, independently of that family's factory case policy.
    /// This is deliberately separate from classifyGeneric(): for example,
    /// `uInT64` is not a valid built-in spelling, but it is a UDT collision.
    [[nodiscard]] static bool collidesWithRegisteredFamilyOrAlias(std::string_view family_name) noexcept;
    [[nodiscard]] static BuiltInDataTypeFamilyClassification classifySpecializedEnum(std::string_view family_name) noexcept;
    [[nodiscard]] static BuiltInDataTypeFamilyClassification classifySpecializedTuple(std::string_view family_name) noexcept;
    [[nodiscard]] static BuiltInDataTypeFamilyClassification classifyQualifiedReference() noexcept;

    [[nodiscard]] static constexpr std::size_t registeredFamilyCount() noexcept { return 141; }
    [[nodiscard]] static constexpr std::size_t maximumFamilyNameSize() noexcept { return 31; }
};

}
