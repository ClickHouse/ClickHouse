#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace DB::UDT
{

struct CanonicalTypeArgumentLimits
{
    UInt64 maximum_ast_nodes = 4'096;
    UInt64 maximum_ast_edges = 65'536;
    UInt64 maximum_ast_depth = 32;
    UInt64 maximum_field_nodes = 4'096;
    UInt64 maximum_field_edges = 65'536;
    UInt64 maximum_field_depth = 32;
    UInt64 maximum_owned_string_bytes = 1ULL << 20;
    UInt64 maximum_enum_entries = 1ULL << 16;
    UInt64 maximum_qbit_materialized_streams = 1ULL << 16;
};

/// Validate caller-supplied admission limits without inspecting an argument
/// or invoking DataTypeFactory.
void validateCanonicalTypeArgumentLimits(const CanonicalTypeArgumentLimits & limits);

struct CanonicalTypeArgumentAdmissionStatistics
{
    UInt64 ast_node_occurrences = 0;
    UInt64 ast_edges = 0;
    UInt64 maximum_ast_depth = 0;
    UInt64 field_node_occurrences = 0;
    UInt64 field_edges = 0;
    UInt64 maximum_field_depth = 0;
    UInt64 owned_string_bytes = 0;
    UInt64 enum_entries = 0;
    UInt64 qbit_materialized_streams = 0;
    UInt64 generic_enums_canonicalized = 0;
    UInt64 factory_calls = 0;

    bool operator==(const CanonicalTypeArgumentAdmissionStatistics &) const = default;
};

class TemplateSpecializer;
class CanonicalTypeArguments;

/// TYPE actuals are admitted from a structured parser AST, prospectively
/// bounded, deep-cloned away from caller ownership, and validated exactly once
/// by DataTypeFactory. The retained AST is canonical resolver-owned structure;
/// formatted SQL is neither accepted as authority nor reparsed.
class CanonicalTypeArgument final
{
public:
    static CanonicalTypeArgument fromFactoryValidatedAST(
        const ASTPtr & type_ast,
        const CanonicalTypeArgumentLimits & limits = {},
        CanonicalTypeArgumentAdmissionStatistics * statistics = nullptr);

    const DataTypePtr & getPhysicalType() const noexcept { return physical_type; }
    const String & getCanonicalName() const noexcept { return canonical_name; }
    const String & getBinaryEncoding() const noexcept { return binary_encoding; }

    bool operator==(const CanonicalTypeArgument & rhs) const noexcept { return binary_encoding == rhs.binary_encoding; }

private:
    CanonicalTypeArgument(ASTPtr canonical_ast_, DataTypePtr physical_type_, String canonical_name_, String binary_encoding_)
        : canonical_ast(std::move(canonical_ast_))
        , physical_type(std::move(physical_type_))
        , canonical_name(std::move(canonical_name_))
        , binary_encoding(std::move(binary_encoding_))
    {
    }

    const ASTPtr & getCanonicalASTForSpecialization() const noexcept { return canonical_ast; }

    friend class TemplateSpecializer;
    friend class CanonicalTypeArguments;

    ASTPtr canonical_ast;
    DataTypePtr physical_type;
    String canonical_name;
    String binary_encoding;
};

/*
 * Keep construction of TYPE actuals on the structured boundary. There is
 * deliberately no DataTypePtr-only constructor: recovering an AST from
 * IDataType::getName() would be the forbidden format-to-parse path.
 */

using CanonicalArgumentValue = std::variant<CanonicalTypeArgument, bool, UInt64, Int64, String>;

struct CanonicalTypeArgumentValue
{
    ParameterKind kind = ParameterKind::Type;
    CanonicalArgumentValue value;

    static CanonicalTypeArgumentValue type(const ASTPtr & type_ast, const CanonicalTypeArgumentLimits & limits = {});
    static CanonicalTypeArgumentValue boolean(bool boolean_value);
    static CanonicalTypeArgumentValue unsignedInteger(ParameterKind parameter_kind, UInt64 integer_value);
    static CanonicalTypeArgumentValue signedInteger(ParameterKind parameter_kind, Int64 integer_value);
    static CanonicalTypeArgumentValue string(String string_value);

    bool operator==(const CanonicalTypeArgumentValue &) const noexcept = default;
};

class CanonicalTypeArguments final
{
public:
    static CanonicalTypeArguments validate(
        std::span<const Parameter> parameters,
        std::vector<CanonicalTypeArgumentValue> actuals,
        UInt64 maximum_total_bytes = 64ULL << 10,
        UInt64 maximum_item_bytes = 16ULL << 10);

    /// Decode the frozen V1 argument bytes. TYPE frames are admitted through a
    /// bounded parser-surface reconstruction before the single factory call.
    static CanonicalTypeArguments decode(
        std::span<const Parameter> parameters,
        std::string_view encoded,
        UInt64 maximum_total_bytes = 64ULL << 10,
        UInt64 maximum_item_bytes = 16ULL << 10,
        const CanonicalTypeArgumentLimits & type_limits = {});

    const std::vector<CanonicalTypeArgumentValue> & values() const noexcept { return actuals; }
    const String & encoded() const noexcept { return canonical_encoding; }

    bool operator==(const CanonicalTypeArguments &) const noexcept = default;

private:
    CanonicalTypeArguments(std::vector<CanonicalTypeArgumentValue> actuals_, String canonical_encoding_)
        : actuals(std::move(actuals_))
        , canonical_encoding(std::move(canonical_encoding_))
    {
    }

    std::vector<CanonicalTypeArgumentValue> actuals;
    String canonical_encoding;
};

}
