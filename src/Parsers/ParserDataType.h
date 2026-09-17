#pragma once
#include <Parsers/IParserBase.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

enum class UDTParameterKind : UInt8;

enum class DataTypeFamilySyntaxKind : uint8_t
{
    Generic,
    SpecializedEnum,
    SpecializedTuple,
    QualifiedReference,
};

struct DataTypeFamilyClassification
{
    bool is_built_in = false;
    bool is_qualified_reference = false;
};

/// Compact result accumulated while the parser is already constructing the type tree.
/// It is deliberately caller-owned and is never attached to the AST.
struct DataTypeFamilyClassificationSummary
{
    enum Flag : uint8_t
    {
        HasLogicalFamily = 1,
        HasQualifiedLogicalFamily = 2,
    };

    uint8_t flags = 0;

    void add(const DataTypeFamilyClassification & classification) noexcept
    {
        if (!classification.is_built_in)
        {
            flags |= HasLogicalFamily;
            if (classification.is_qualified_reference)
                flags |= HasQualifiedLogicalFamily;
        }
    }

    void merge(const DataTypeFamilyClassificationSummary & child) noexcept { flags |= child.flags; }

    bool allFamiliesAreBuiltIn() const noexcept { return !(flags & HasLogicalFamily); }
    bool hasQualifiedLogicalFamily() const noexcept { return flags & HasQualifiedLogicalFamily; }
};

static_assert(sizeof(DataTypeFamilyClassificationSummary) == sizeof(uint8_t));

/// A non-owning, type-erased classifier. The callback is intentionally non-nullable on
/// the classified parser path, so every family costs one indirect call but no branch,
/// allocation, lock, or registry ownership transfer.
struct DataTypeFamilyClassifier
{
    using Callback = DataTypeFamilyClassification (*)(
        const void * context, std::string_view family_name, DataTypeFamilySyntaxKind syntax_kind) noexcept;

    const void * context;
    Callback callback;

    DataTypeFamilyClassification classify(std::string_view family_name, DataTypeFamilySyntaxKind syntax_kind) const noexcept
    {
        return callback(context, family_name, syntax_kind);
    }
};

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Column-declaration parser variant which additionally emits an
/// ASTUDTReference for an exact two-component qualified type name.
/// The default ParserDataType deliberately keeps its original syntax surface.
class ParserDataTypeWithQualifiedReferences : public IParserBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Opt-in parser variant which classifies each emitted family during the existing
/// construction walk. Failed/backtracked parses do not modify `summary`.
class ParserDataTypeWithFamilyClassification : public IParserBase
{
public:
    ParserDataTypeWithFamilyClassification(DataTypeFamilyClassifier classifier_, DataTypeFamilyClassificationSummary & summary_)
        : classifier(classifier_)
        , summary(summary_)
    {
    }

protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    DataTypeFamilyClassifier classifier;
    DataTypeFamilyClassificationSummary & summary;
};

struct UDTTemplateParameterDescriptor
{
    String name;
    UInt16 ordinal = 0;
    UDTParameterKind kind = static_cast<UDTParameterKind>(0);
};

struct UDTParameterNameHash
{
    using is_transparent = void;

    size_t operator()(std::string_view value) const noexcept { return std::hash<std::string_view>{}(value); }
};

/// Context required to parse a checked-template body. It is owned by the parser
/// so recursive IParser instances never borrow a transient declaration vector.
struct UDTExpressionParserContext
{
    std::vector<UDTTemplateParameterDescriptor> parameters;
    String definition_database;
    String definition_name;
    std::optional<UInt16> decreasing_parameter;
};

/// CREATE TYPE-only data-type parser. Unlike ParserDataType, it recognizes the
/// declared formal table and emits compile-time AST nodes for parameter uses,
/// TYPE_IF, and the single approved decreasing self-call expression.
class ParserUDTExpression : public IParserBase
{
public:
    explicit ParserUDTExpression(UDTExpressionParserContext context_);

protected:
    const char * getName() const override { return "user-defined type expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    UDTExpressionParserContext context;
    std::unordered_map<String, size_t, UDTParameterNameHash, std::equal_to<>> parameter_indexes;
    bool context_is_valid = true;
};
}
