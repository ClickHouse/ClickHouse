#include <DataTypes/UDT/TemplateSpecializer.h>

#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/UDT/CanonicalHash.h>

#include <Common/Exception.h>

#include <Core/Field.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/NullsAction.h>

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace DB::UDT
{
namespace
{

using ErrorCode = TemplateSpecializerError::Code;

[[noreturn]] void fail(ErrorCode code, std::string_view message)
{
    throw TemplateSpecializerError(code, message);
}

UInt64 checkedSize(std::size_t size, std::string_view description)
{
    if (!std::in_range<UInt64>(size))
        fail(ErrorCode::LimitExceeded, description);
    return static_cast<UInt64>(size);
}

void addProspectively(UInt64 & current, UInt64 amount, UInt64 maximum, std::string_view description)
{
    if (amount > maximum || current > maximum - amount)
        fail(ErrorCode::LimitExceeded, description);
    current += amount;
}

void ensureProspective(UInt64 current, UInt64 amount, UInt64 maximum, std::string_view description)
{
    if (amount > maximum || current > maximum - amount)
        fail(ErrorCode::LimitExceeded, description);
}

UInt64 checkedProduct(UInt64 lhs, UInt64 rhs, std::string_view description)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(ErrorCode::LimitExceeded, description);
    return lhs * rhs;
}

void validateLimits(const TemplateSpecializerLimits & limits)
{
    constexpr TemplateSpecializerLimits implementation_maxima;
    const auto validate = [](UInt64 value, UInt64 maximum, std::string_view name)
    {
        if (value == 0)
            fail(ErrorCode::InvalidArguments, fmt::format("{} must be nonzero", name));
        if (value > maximum)
            fail(ErrorCode::InvalidArguments, fmt::format("{} exceeds the implementation maximum", name));
    };

    validate(
        limits.maximum_distinct_specializations,
        implementation_maxima.maximum_distinct_specializations,
        "maximum_distinct_specializations");
    validate(limits.maximum_definition_handles, implementation_maxima.maximum_definition_handles, "maximum_definition_handles");
    validate(limits.maximum_definition_lookups, implementation_maxima.maximum_definition_lookups, "maximum_definition_lookups");
    validate(limits.maximum_specialization_depth, implementation_maxima.maximum_specialization_depth, "maximum_specialization_depth");
    validate(
        limits.maximum_canonical_argument_bytes,
        implementation_maxima.maximum_canonical_argument_bytes,
        "maximum_canonical_argument_bytes");
    validate(
        limits.maximum_canonical_argument_item_bytes,
        implementation_maxima.maximum_canonical_argument_item_bytes,
        "maximum_canonical_argument_item_bytes");
    validate(limits.maximum_memo_key_bytes, implementation_maxima.maximum_memo_key_bytes, "maximum_memo_key_bytes");
    validate(
        limits.maximum_template_node_occurrences,
        implementation_maxima.maximum_template_node_occurrences,
        "maximum_template_node_occurrences");
    validate(limits.maximum_constructed_ast_nodes, implementation_maxima.maximum_constructed_ast_nodes, "maximum_constructed_ast_nodes");
    validate(limits.maximum_constructed_ast_edges, implementation_maxima.maximum_constructed_ast_edges, "maximum_constructed_ast_edges");
    validate(limits.maximum_ast_depth, implementation_maxima.maximum_ast_depth, "maximum_ast_depth");
    validate(limits.maximum_field_depth, implementation_maxima.maximum_field_depth, "maximum_field_depth");
    validate(limits.maximum_owned_ast_string_bytes, implementation_maxima.maximum_owned_ast_string_bytes, "maximum_owned_ast_string_bytes");
    validate(limits.maximum_enum_entries, implementation_maxima.maximum_enum_entries, "maximum_enum_entries");
    validate(limits.maximum_retained_occurrences, implementation_maxima.maximum_retained_occurrences, "maximum_retained_occurrences");
    validate(
        limits.maximum_retained_path_components,
        implementation_maxima.maximum_retained_path_components,
        "maximum_retained_path_components");
    validate(
        limits.maximum_emitted_ast_node_occurrences,
        implementation_maxima.maximum_emitted_ast_node_occurrences,
        "maximum_emitted_ast_node_occurrences");
    validate(limits.maximum_emitted_ast_edges, implementation_maxima.maximum_emitted_ast_edges, "maximum_emitted_ast_edges");
    validate(limits.maximum_emitted_occurrences, implementation_maxima.maximum_emitted_occurrences, "maximum_emitted_occurrences");
    validate(
        limits.maximum_emitted_path_components, implementation_maxima.maximum_emitted_path_components, "maximum_emitted_path_components");
    validate(limits.maximum_work, implementation_maxima.maximum_work, "maximum_work");

    if (limits.maximum_canonical_argument_item_bytes > limits.maximum_canonical_argument_bytes)
        fail(ErrorCode::InvalidArguments, "canonical argument item limit exceeds the total limit");
    if (limits.maximum_distinct_specializations >= invalid_template_specialization_id)
        fail(ErrorCode::InvalidArguments, "specialization limit exceeds the ID domain");
}

UInt64 mix(UInt64 value) noexcept
{
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

struct DefinitionIdentityHash
{
    std::size_t operator()(const DefinitionIdentity & identity) const
    {
        CanonicalHasher hash("ClickHouse UDT specialization identity table V1");
        hash.updateUUID(identity.database_uuid);
        hash.updateUUID(identity.type_uuid);
        std::array<CanonicalByte, sizeof(UInt64)> revision{};
        for (std::size_t index = 0; index < revision.size(); ++index)
            revision[index] = static_cast<CanonicalByte>(identity.revision >> (8 * index));
        hash.update(revision);
        const Digest digest = hash.finalize();
        UInt64 result = 0;
        for (std::size_t index = 0; index < sizeof(result); ++index)
            result = (result << 8) | digest[index];
        return static_cast<std::size_t>(result);
    }
};

bool identityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs)
{
    const auto lhs_database = uuidToCanonicalBytes(lhs.database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs.database_uuid);
    if (lhs_database != rhs_database)
        return lhs_database < rhs_database;
    const auto lhs_type = uuidToCanonicalBytes(lhs.type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs.type_uuid);
    if (lhs_type != rhs_type)
        return lhs_type < rhs_type;
    return lhs.revision < rhs.revision;
}

struct MemoKey
{
    DefinitionIdentity identity;
    Digest canonical_arguments_digest{};
    std::string_view canonical_arguments;

    bool operator==(const MemoKey &) const = default;
};

struct MemoKeyView
{
    const DefinitionIdentity & identity;
    const Digest & canonical_arguments_digest;
    std::string_view canonical_arguments;
};

UInt64 hashDigest(const Digest & value) noexcept
{
    UInt64 result = 0x9e3779b97f4a7c15ULL;
    for (const UInt8 byte : value)
        result = mix(result ^ byte);
    return result;
}

struct MemoKeyHash
{
    using is_transparent = void;

    std::size_t operator()(const MemoKey & key) const
    {
        return (*this)(MemoKeyView{key.identity, key.canonical_arguments_digest, key.canonical_arguments});
    }

    std::size_t operator()(const MemoKeyView & key) const
    {
        return static_cast<std::size_t>(mix(DefinitionIdentityHash{}(key.identity) ^ hashDigest(key.canonical_arguments_digest)));
    }
};

struct MemoKeyEqual
{
    using is_transparent = void;

    bool operator()(const MemoKey & lhs, const MemoKey & rhs) const noexcept { return lhs == rhs; }
    bool operator()(const MemoKey & lhs, const MemoKeyView & rhs) const noexcept
    {
        return lhs.identity == rhs.identity && lhs.canonical_arguments_digest == rhs.canonical_arguments_digest
            && lhs.canonical_arguments == rhs.canonical_arguments;
    }
};

struct PhysicalASTCost
{
    UInt64 nodes = 0;
    UInt64 edges = 0;
    UInt64 depth = 0;
};

struct ExpandedNode
{
    ASTPtr ast;
    PhysicalASTCost cost;
    std::vector<RelativeLogicalTypeOccurrence> occurrences;
};

enum class MemoState : UInt8
{
    Active,
    Complete,
};

struct MemoEntry
{
    MemoEntry(
        TemplateSpecializationID id_, DefinitionIdentity identity_, UInt32 definition_handle_index_, CanonicalTypeArguments arguments_)
        : id(id_)
        , identity(identity_)
        , definition_handle_index(definition_handle_index_)
        , arguments(std::move(arguments_))
    {
    }

    TemplateSpecializationID id;
    DefinitionIdentity identity;
    UInt32 definition_handle_index;
    CanonicalTypeArguments arguments;
    MemoState state = MemoState::Active;
    ASTPtr ast;
    PhysicalASTCost cost;
    std::vector<RelativeLogicalTypeOccurrence> occurrences;
};

bool isTypeProducingNode(TemplateNodeKind kind) noexcept
{
    return kind == TemplateNodeKind::BuiltIn || kind == TemplateNodeKind::TypeParameter || kind == TemplateNodeKind::SpecializedEnum
        || kind == TemplateNodeKind::TypeIfZero || kind == TemplateNodeKind::SelfCall || kind == TemplateNodeKind::DefinitionCall;
}

bool binaryStringLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool identityIsValid(const DefinitionIdentity & identity) noexcept
{
    return identity.database_uuid != UUIDHelpers::Nil && identity.type_uuid != UUIDHelpers::Nil && identity.revision != 0;
}

constexpr UInt64 memo_identity_bytes = 16 + 16 + sizeof(UInt64);

template <typename Value>
Value readLittleEndianExact(std::string_view payload, std::string_view description)
{
    if (payload.size() != sizeof(Value))
        fail(ErrorCode::InvalidTemplate, description);
    ReadBufferFromMemory input(payload.data(), payload.size());
    Value result{};
    readBinaryLittleEndian(result, input);
    if (!input.eof())
        fail(ErrorCode::InvalidTemplate, description);
    return result;
}

template <typename Decimal>
DecimalField<Decimal> readDecimalExact(std::string_view payload, std::string_view description)
{
    constexpr std::size_t expected_size = sizeof(Decimal) + sizeof(UInt32);
    if (payload.size() != expected_size)
        fail(ErrorCode::InvalidTemplate, description);
    ReadBufferFromMemory input(payload.data(), payload.size());
    Decimal value{};
    UInt32 scale = 0;
    readBinaryLittleEndian(value, input);
    readBinaryLittleEndian(scale, input);
    if (!input.eof())
        fail(ErrorCode::InvalidTemplate, description);
    return DecimalField<Decimal>(value, scale);
}

NullsAction toParserNullsAction(AggregateFunctionNullsAction action)
{
    switch (action)
    {
        case AggregateFunctionNullsAction::Empty: return NullsAction::EMPTY;
        case AggregateFunctionNullsAction::RespectNulls: return NullsAction::RESPECT_NULLS;
        case AggregateFunctionNullsAction::IgnoreNulls: return NullsAction::IGNORE_NULLS;
    }
    fail(ErrorCode::InvalidTemplate, "aggregate-function nulls action is unknown");
}

}

TemplateSpecializerError::TemplateSpecializerError(Code code_, std::string_view message)
    : std::runtime_error(fmt::format("User-defined type template specialization failed: {}", message))
    , code(code_)
{
}

void validateTemplateSpecializerLimits(const TemplateSpecializerLimits & limits)
{
    validateLimits(limits);
}

class TemplateSpecializer::Attempt::State
{
public:
    State(
        UUID database_uuid_,
        TypeAuthorityCapabilities capabilities_,
        TemplateSpecializerLimits limits_,
        IAuthorityAdapter::ResolutionSession session_,
        ProspectiveResourceBudget * query_budget_,
        bool query_memo_retention_enabled_)
        : database_uuid(database_uuid_)
        , capabilities(std::move(capabilities_))
        , limits(std::move(limits_))
        , authority_generation(session_.getGeneration())
        , session(std::make_unique<IAuthorityAdapter::ResolutionSession>(std::move(session_)))
        , query_budget(query_budget_)
        , query_memo_retention_enabled(query_memo_retention_enabled_)
    {
        statistics.resolution_sessions = 1;
        const auto initial_specializations = static_cast<std::size_t>(std::min<UInt64>(32, limits.maximum_distinct_specializations));
        const auto initial_definitions = static_cast<std::size_t>(std::min<UInt64>(16, limits.maximum_definition_handles));
        memo.reserve(initial_specializations);
        entries.reserve(initial_specializations);
        definition_handle_indexes.reserve(initial_definitions);
        definition_handles.reserve(initial_definitions);
        canonical_ast_costs.reserve(initial_specializations);
    }

    TemplateSpecializationID specialize(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments);
    TemplateSpecializationID specializeFromQueryMemo(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments);
    TemplateSpecializationID specializeEncoded(
        const DefinitionIdentity & identity,
        std::string_view canonical_arguments,
        const CanonicalTypeArgumentLimits & type_argument_limits);
    const ASTPtr & getCanonicalPhysicalAST(TemplateSpecializationID id) const;
    TemplateSpecializationView getSpecialization(TemplateSpecializationID id) const;
    const TemplateSpecializerStatistics & getStatistics() const noexcept { return statistics; }
    const TemplateSpecializerLimits & getLimits() const noexcept { return limits; }
    UUID getAuthorityDatabaseUUID() const noexcept { return database_uuid; }
    UInt64 getAuthorityGeneration() const noexcept { return authority_generation; }
    static void chargeInitialQueryMemoRetention(const TemplateSpecializerLimits & limits, ProspectiveResourceBudget & query_budget);
    void attachResolutionSession(IAuthorityAdapter::ResolutionSession session_);
    void releaseResolutionSession() noexcept { session.reset(); }
    FinishedTemplateSpecializations finish();

private:
    struct ExpandedField
    {
        Field field;
    };

    struct BuiltInChild
    {
        TemplateNodeKind kind;
        std::string_view label;
        ExpandedNode expansion;
    };

    template <typename AST, typename... Args>
    boost::intrusive_ptr<AST> makeASTNode(Args &&... args)
    {
        chargeConstructedNodes(1);
        return make_intrusive<AST>(std::forward<Args>(args)...);
    }

    void chargeWork(UInt64 amount = 1);
    void chargeConstructedNodes(UInt64 amount);
    void chargeConstructedEdges(UInt64 amount);
    void chargeASTString(std::string_view value);
    void chargeEnumEntries(UInt64 amount);
    void chargeTemplateNode();
    void checkASTDepth(UInt64 depth);
    PhysicalASTCost composeCost(
        UInt64 own_nodes, UInt64 own_edges, UInt64 own_depth, UInt64 child_depth_offset, std::span<const PhysicalASTCost> children) const;
    void chargeEmitted(const MemoEntry & entry);
    Definition::Ptr resolveDefinition(const DefinitionIdentity & identity, UInt32 * handle_index = nullptr);
    void preflightDefinitionRetention() const;
    void validateDefinitionRetentionLimits(const Definition & definition) const;
    UInt32 retainDefinition(const Definition::Ptr & definition);
    CanonicalTypeArguments validateArguments(const Definition & definition, const CanonicalTypeArguments & arguments) const;
    PhysicalASTCost inspectCanonicalAST(const ASTPtr & root, UInt64 depth);
    ExpandedField expandFieldValue(
        const Definition & definition,
        const CanonicalTypeArguments & arguments,
        TemplateNodeID node_id,
        UInt64 specialization_depth,
        UInt64 field_depth,
        bool charge_root);
    ASTPtr makeLiteralAST(Field value);
    ASTPtr makeIdentifierAST(std::string_view name);
    ASTPtr makeFunctionAST(std::string_view name, std::vector<ASTPtr> arguments, NullsAction nulls_action = NullsAction::EMPTY);
    ExpandedNode expandBuiltIn(
        const Definition & definition,
        const CanonicalTypeArguments & arguments,
        const TemplateNode & node,
        UInt64 specialization_depth,
        UInt64 ast_depth);
    void appendOccurrences(
        std::vector<RelativeLogicalTypeOccurrence> & destination,
        const std::vector<RelativeLogicalTypeOccurrence> & source,
        std::optional<PhysicalTypeChildLocator> prefix,
        const std::vector<TemplateNodeChild> * type_argument_mapping = nullptr);
    void prefixAndMoveOccurrences(
        std::vector<RelativeLogicalTypeOccurrence> & destination,
        std::vector<RelativeLogicalTypeOccurrence> source,
        std::optional<PhysicalTypeChildLocator> prefix);
    void chargeRetainedOccurrence(UInt64 path_components);
    void chargeRetainedOccurrence(const RelativeLogicalTypeOccurrence & occurrence);
    void chargeQueryMemoScratch(UInt64 bytes);
    void chargeQueryMemoKey(const CanonicalTypeArguments & arguments);
    static bool argumentIsZero(const CanonicalTypeArgumentValue & argument);
    CanonicalTypeArguments
    decrementedArguments(const Definition & definition, const CanonicalTypeArguments & arguments, const TemplateNode & node);
    CanonicalTypeArguments
    dependencyArguments(const Definition & target, const CanonicalTypeArguments & caller_arguments, const TemplateNode & call);

    TemplateSpecializationID specializeDefinition(
        const DefinitionIdentity & identity,
        const CanonicalTypeArguments & arguments,
        UInt64 specialization_depth,
        const Digest * precomputed_arguments_digest = nullptr);
    ExpandedNode expandNode(
        const Definition & definition,
        const CanonicalTypeArguments & arguments,
        TemplateNodeID node_id,
        UInt64 specialization_depth,
        UInt64 ast_depth);
    UUID database_uuid;
    TypeAuthorityCapabilities capabilities;
    TemplateSpecializerLimits limits;
    UInt64 authority_generation = 0;
    std::unique_ptr<IAuthorityAdapter::ResolutionSession> session;
    TemplateSpecializerStatistics statistics;
    ProspectiveResourceBudget * query_budget = nullptr;
    bool query_memo_retention_enabled = false;
    absl::flat_hash_map<MemoKey, TemplateSpecializationID, MemoKeyHash, MemoKeyEqual> memo;
    std::vector<std::unique_ptr<MemoEntry>> entries;
    absl::flat_hash_map<DefinitionIdentity, UInt32, DefinitionIdentityHash> definition_handle_indexes;
    std::vector<Definition::Ptr> definition_handles;
    absl::flat_hash_map<const IAST *, PhysicalASTCost> canonical_ast_costs;
};

void TemplateSpecializer::Attempt::State::chargeInitialQueryMemoRetention(
    const TemplateSpecializerLimits & limits, ProspectiveResourceBudget & query_budget)
{
    const UInt64 initial_specializations = std::min<UInt64>(32, limits.maximum_distinct_specializations);
    const UInt64 initial_definitions = std::min<UInt64>(16, limits.maximum_definition_handles);
    UInt64 retained_bytes = sizeof(State) + sizeof(IAuthorityAdapter::ResolutionSession);
    addProspectively(
        retained_bytes,
        checkedProduct(
            initial_specializations,
            sizeof(std::unique_ptr<MemoEntry>) + sizeof(MemoKey) + sizeof(TemplateSpecializationID)
                + sizeof(std::pair<const IAST * const, PhysicalASTCost>),
            "query specialization memo base retention overflows UInt64"),
        std::numeric_limits<UInt64>::max(),
        "query specialization memo base retention overflows UInt64");
    addProspectively(
        retained_bytes,
        checkedProduct(
            initial_definitions,
            sizeof(Definition::Ptr) + sizeof(std::pair<const DefinitionIdentity, UInt32>),
            "query specialization memo definition-index retention overflows UInt64"),
        std::numeric_limits<UInt64>::max(),
        "query specialization memo definition-index retention overflows UInt64");

    const auto admission = query_budget.charge(ResourceLimit::SemanticScratchBytesPerQuery, retained_bytes);
    if (!admission.isAccepted())
        fail(ErrorCode::LimitExceeded, formatResourceAdmissionFailure(admission));
}

void TemplateSpecializer::Attempt::State::attachResolutionSession(IAuthorityAdapter::ResolutionSession session_)
{
    if (session)
        fail(ErrorCode::InvalidAttemptState, "query specialization memo already has an active authority session");
    if (session_.getGeneration() != authority_generation)
        fail(ErrorCode::AuthorityFailure, "query specialization memo authority generation changed");
    session = std::make_unique<IAuthorityAdapter::ResolutionSession>(std::move(session_));
    if (statistics.resolution_sessions == std::numeric_limits<UInt64>::max())
        fail(ErrorCode::LimitExceeded, "query specialization resolution-session count overflows UInt64");
    ++statistics.resolution_sessions;
}

void TemplateSpecializer::Attempt::State::chargeQueryMemoScratch(UInt64 bytes)
{
    if (!query_memo_retention_enabled || bytes == 0)
        return;
    if (!query_budget)
        fail(ErrorCode::InvalidAttemptState, "query memo retention has no query resource budget");
    const auto admission = query_budget->charge(ResourceLimit::SemanticScratchBytesPerQuery, bytes);
    if (!admission.isAccepted())
        fail(ErrorCode::LimitExceeded, formatResourceAdmissionFailure(admission));
}

void TemplateSpecializer::Attempt::State::chargeQueryMemoKey(const CanonicalTypeArguments & arguments)
{
    UInt64 retained_bytes = sizeof(MemoEntry) + sizeof(std::unique_ptr<MemoEntry>) + sizeof(MemoKey)
        + sizeof(TemplateSpecializationID) + 2 * sizeof(void *);
    addProspectively(
        retained_bytes,
        checkedSize(arguments.encoded().size(), "query specialization memo argument bytes do not fit UInt64"),
        std::numeric_limits<UInt64>::max(),
        "query specialization memo key retention overflows UInt64");
    addProspectively(
        retained_bytes,
        checkedProduct(
            checkedSize(arguments.values().size(), "query specialization memo argument count does not fit UInt64"),
            sizeof(CanonicalTypeArgumentValue),
            "query specialization memo argument retention overflows UInt64"),
        std::numeric_limits<UInt64>::max(),
        "query specialization memo key retention overflows UInt64");
    for (const auto & argument : arguments.values())
    {
        if (argument.kind == ParameterKind::String && std::holds_alternative<String>(argument.value))
        {
            addProspectively(
                retained_bytes,
                checkedSize(std::get<String>(argument.value).size(), "query specialization string argument bytes do not fit UInt64"),
                std::numeric_limits<UInt64>::max(),
                "query specialization memo key retention overflows UInt64");
        }
        else if (argument.kind == ParameterKind::Type && std::holds_alternative<CanonicalTypeArgument>(argument.value))
        {
            const auto & type = std::get<CanonicalTypeArgument>(argument.value);
            addProspectively(
                retained_bytes,
                checkedSize(type.getCanonicalName().size(), "query specialization type name bytes do not fit UInt64"),
                std::numeric_limits<UInt64>::max(),
                "query specialization memo key retention overflows UInt64");
            addProspectively(
                retained_bytes,
                checkedSize(type.getBinaryEncoding().size(), "query specialization type encoding bytes do not fit UInt64"),
                std::numeric_limits<UInt64>::max(),
                "query specialization memo key retention overflows UInt64");
        }
    }
    chargeQueryMemoScratch(retained_bytes);
}

void TemplateSpecializer::Attempt::State::chargeWork(UInt64 amount)
{
    ensureProspective(statistics.charged_work, amount, limits.maximum_work, "specialization work");
    if (query_budget)
    {
        const auto admission = query_budget->charge(ResourceLimit::CheckerExpansionWorkUnits, amount);
        if (!admission.isAccepted())
            fail(ErrorCode::LimitExceeded, formatResourceAdmissionFailure(admission));
    }
    statistics.charged_work += amount;
}

void TemplateSpecializer::Attempt::State::chargeConstructedNodes(UInt64 amount)
{
    addProspectively(statistics.constructed_ast_nodes, amount, limits.maximum_constructed_ast_nodes, "constructed AST node count");
    chargeWork(amount);
    constexpr UInt64 maximum_node_bytes = std::max(
        {sizeof(ASTDataType),
         sizeof(ASTEnumDataType),
         sizeof(ASTExpressionList),
         sizeof(ASTFunction),
         sizeof(ASTIdentifier),
         sizeof(ASTLiteral),
         sizeof(ASTNameTypePair),
         sizeof(ASTObjectTypeArgument),
         sizeof(ASTObjectTypedPathArgument),
         sizeof(ASTTupleDataType)});
    chargeQueryMemoScratch(
        checkedProduct(amount, maximum_node_bytes + sizeof(ASTPtr), "query specialization AST-node retention overflows UInt64"));
}

void TemplateSpecializer::Attempt::State::chargeConstructedEdges(UInt64 amount)
{
    addProspectively(statistics.constructed_ast_edges, amount, limits.maximum_constructed_ast_edges, "constructed AST edge count");
    chargeWork(amount);
    chargeQueryMemoScratch(checkedProduct(amount, sizeof(ASTPtr), "query specialization AST-edge retention overflows UInt64"));
}

void TemplateSpecializer::Attempt::State::chargeASTString(std::string_view value)
{
    addProspectively(
        statistics.owned_ast_string_bytes,
        checkedSize(value.size(), "owned AST string length does not fit UInt64"),
        limits.maximum_owned_ast_string_bytes,
        "owned AST string bytes");
    chargeQueryMemoScratch(checkedSize(value.size(), "query specialization AST string bytes do not fit UInt64"));
}

void TemplateSpecializer::Attempt::State::chargeEnumEntries(UInt64 amount)
{
    addProspectively(statistics.enum_entries, amount, limits.maximum_enum_entries, "specialized Enum entries");
    chargeWork(amount);
    chargeQueryMemoScratch(
        checkedProduct(amount, sizeof(std::pair<String, Int16>), "query specialization Enum retention overflows UInt64"));
}

void TemplateSpecializer::Attempt::State::chargeTemplateNode()
{
    addProspectively(statistics.template_node_occurrences, 1, limits.maximum_template_node_occurrences, "template node occurrences");
    chargeWork();
}

void TemplateSpecializer::Attempt::State::checkASTDepth(UInt64 depth)
{
    if (depth > limits.maximum_ast_depth)
        fail(ErrorCode::LimitExceeded, "constructed AST depth exceeds its limit");
    statistics.maximum_ast_depth = std::max(statistics.maximum_ast_depth, depth);
}

PhysicalASTCost TemplateSpecializer::Attempt::State::composeCost(
    UInt64 own_nodes, UInt64 own_edges, UInt64 own_depth, UInt64 child_depth_offset, std::span<const PhysicalASTCost> children) const
{
    PhysicalASTCost result{.nodes = own_nodes, .edges = own_edges, .depth = own_depth};
    for (const auto & child : children)
    {
        if (child.nodes > std::numeric_limits<UInt64>::max() - result.nodes
            || child.edges > std::numeric_limits<UInt64>::max() - result.edges
            || child.depth > std::numeric_limits<UInt64>::max() - child_depth_offset)
            fail(ErrorCode::LimitExceeded, "physical AST cost overflows UInt64");
        result.nodes += child.nodes;
        result.edges += child.edges;
        result.depth = std::max(result.depth, child_depth_offset + child.depth);
    }
    return result;
}

ASTPtr TemplateSpecializer::Attempt::State::makeLiteralAST(Field value)
{
    return makeASTNode<ASTLiteral>(std::move(value));
}

ASTPtr TemplateSpecializer::Attempt::State::makeIdentifierAST(std::string_view name)
{
    chargeASTString(name);
    return makeASTNode<ASTIdentifier>(String(name));
}

ASTPtr TemplateSpecializer::Attempt::State::makeFunctionAST(std::string_view name, std::vector<ASTPtr> arguments, NullsAction nulls_action)
{
    chargeASTString(name);
    chargeConstructedNodes(2);
    chargeConstructedEdges(1 + checkedSize(arguments.size(), "function argument count does not fit UInt64"));
    auto function = make_intrusive<ASTFunction>();
    function->name = String(name);
    function->setNullsAction(nulls_action);
    auto argument_list = make_intrusive<ASTExpressionList>();
    argument_list->children.reserve(arguments.size());
    argument_list->children.insert(
        argument_list->children.end(), std::make_move_iterator(arguments.begin()), std::make_move_iterator(arguments.end()));
    function->arguments = argument_list;
    function->children.push_back(std::move(argument_list));
    return function;
}

PhysicalASTCost TemplateSpecializer::Attempt::State::inspectCanonicalAST(const ASTPtr & root, UInt64 depth)
{
    if (!root)
        fail(ErrorCode::InvalidArguments, "canonical TYPE argument contains a null root");
    if (const auto found = canonical_ast_costs.find(root.get()); found != canonical_ast_costs.end())
        return found->second;

    struct Frame
    {
        const IAST * node = nullptr;
        std::size_t next_child = 0;
        UInt64 depth = 0;
    };
    std::array<Frame, 64> stack{};
    std::size_t stack_size = 1;
    stack.front() = {.node = root.get(), .next_child = 0, .depth = depth};
    PhysicalASTCost result;

    while (stack_size != 0)
    {
        Frame & frame = stack[stack_size - 1];
        if (frame.next_child == 0)
        {
            if (frame.depth > limits.maximum_ast_depth)
                fail(ErrorCode::LimitExceeded, "canonical TYPE argument AST depth exceeds the specialization limit");
            ++result.nodes;
            result.depth = std::max(result.depth, frame.depth - depth + 1);
            chargeWork();
        }
        if (frame.next_child == frame.node->children.size())
        {
            --stack_size;
            continue;
        }

        const ASTPtr & child = frame.node->children[frame.next_child++];
        if (!child)
            fail(ErrorCode::InvalidArguments, "canonical TYPE argument AST contains a null child");
        if (result.edges == std::numeric_limits<UInt64>::max())
            fail(ErrorCode::LimitExceeded, "canonical TYPE argument AST edge count overflows UInt64");
        ++result.edges;
        chargeWork();
        for (std::size_t ancestor = 0; ancestor < stack_size; ++ancestor)
            if (stack[ancestor].node == child.get())
                fail(ErrorCode::InvalidArguments, "canonical TYPE argument AST contains a cycle");
        if (stack_size >= stack.size())
            fail(ErrorCode::LimitExceeded, "canonical TYPE argument AST depth exceeds the implementation maximum");
        stack[stack_size++] = {.node = child.get(), .next_child = 0, .depth = frame.depth + 1};
    }

    chargeQueryMemoScratch(sizeof(std::pair<const IAST * const, PhysicalASTCost>) + 2 * sizeof(void *));
    canonical_ast_costs.emplace(root.get(), result);
    return result;
}

void TemplateSpecializer::Attempt::State::chargeEmitted(const MemoEntry & entry)
{
    addProspectively(
        statistics.emitted_ast_node_occurrences,
        entry.cost.nodes,
        limits.maximum_emitted_ast_node_occurrences,
        "emitted AST node occurrences");
    addProspectively(statistics.emitted_ast_edges, entry.cost.edges, limits.maximum_emitted_ast_edges, "emitted AST edges");
    addProspectively(
        statistics.emitted_occurrences,
        checkedSize(entry.occurrences.size(), "emitted logical occurrence count does not fit UInt64"),
        limits.maximum_emitted_occurrences,
        "emitted logical occurrences");
    UInt64 path_components = 0;
    for (const auto & occurrence : entry.occurrences)
    {
        const UInt64 size = checkedSize(occurrence.path.size(), "emitted path length does not fit UInt64");
        if (size > std::numeric_limits<UInt64>::max() - path_components)
            fail(ErrorCode::LimitExceeded, "emitted path component count overflows UInt64");
        path_components += size;
    }
    addProspectively(
        statistics.emitted_path_components, path_components, limits.maximum_emitted_path_components, "emitted path components");
    chargeWork(entry.cost.nodes + entry.cost.edges + checkedSize(entry.occurrences.size(), "emitted occurrence count"));
}

void TemplateSpecializer::Attempt::State::chargeRetainedOccurrence(UInt64 path_components)
{
    addProspectively(statistics.retained_occurrences, 1, limits.maximum_retained_occurrences, "retained logical occurrences");
    addProspectively(
        statistics.retained_path_components,
        path_components,
        limits.maximum_retained_path_components,
        "retained path components");
    chargeWork(1 + path_components);
    UInt64 retained_bytes = sizeof(RelativeLogicalTypeOccurrence);
    addProspectively(
        retained_bytes,
        checkedProduct(path_components, sizeof(PhysicalTypeChildLocator), "query specialization occurrence path retention overflows UInt64"),
        std::numeric_limits<UInt64>::max(),
        "query specialization occurrence retention overflows UInt64");
    chargeQueryMemoScratch(retained_bytes);
}

void TemplateSpecializer::Attempt::State::chargeRetainedOccurrence(const RelativeLogicalTypeOccurrence & occurrence)
{
    chargeRetainedOccurrence(checkedSize(occurrence.path.size(), "retained path length does not fit UInt64"));
}

void TemplateSpecializer::Attempt::State::appendOccurrences(
    std::vector<RelativeLogicalTypeOccurrence> & destination,
    const std::vector<RelativeLogicalTypeOccurrence> & source,
    std::optional<PhysicalTypeChildLocator> prefix,
    const std::vector<TemplateNodeChild> * type_argument_mapping)
{
    if (source.size() > destination.max_size() - destination.size())
        fail(ErrorCode::LimitExceeded, "logical occurrence vector exceeds the host size domain");
    const UInt64 added_occurrences = checkedSize(source.size(), "logical occurrence count does not fit UInt64");
    if (added_occurrences > limits.maximum_retained_occurrences - statistics.retained_occurrences)
        fail(ErrorCode::LimitExceeded, "retained logical occurrences");

    UInt64 added_paths = prefix ? added_occurrences : 0;
    for (const auto & occurrence : source)
    {
        if (prefix && occurrence.path.size() == occurrence.path.max_size())
            fail(ErrorCode::LimitExceeded, "logical path exceeds the host size domain");
        if (type_argument_mapping && occurrence.kind == RelativeLogicalTypeOccurrenceKind::TypeArgument)
        {
            if (occurrence.source_ordinal >= type_argument_mapping->size())
                fail(ErrorCode::InvalidTemplate, "definition-call TYPE-argument occurrence is out of range");
            if (!(*type_argument_mapping)[occurrence.source_ordinal].label.empty())
                fail(ErrorCode::InvalidTemplate, "definition-call TYPE-argument mapping carries a label");
        }
        const UInt64 size = checkedSize(occurrence.path.size(), "logical path length does not fit UInt64");
        if (size > std::numeric_limits<UInt64>::max() - added_paths)
            fail(ErrorCode::LimitExceeded, "logical path component count overflows UInt64");
        added_paths += size;
    }
    if (added_paths > limits.maximum_retained_path_components - statistics.retained_path_components)
        fail(ErrorCode::LimitExceeded, "retained path components");
    if (added_paths > std::numeric_limits<UInt64>::max() - added_occurrences)
        fail(ErrorCode::LimitExceeded, "retained occurrence work overflows UInt64");
    ensureProspective(statistics.charged_work, added_occurrences + added_paths, limits.maximum_work, "retained occurrence work");

    for (const auto & occurrence : source)
        chargeRetainedOccurrence(
            checkedSize(occurrence.path.size(), "retained path length does not fit UInt64") + (prefix ? UInt64{1} : UInt64{0}));
    chargeQueryMemoScratch(
        checkedProduct(added_occurrences, sizeof(RelativeLogicalTypeOccurrence), "query specialization occurrence vector overflows UInt64"));
    destination.reserve(destination.size() + source.size());
    for (const auto & occurrence : source)
    {
        RelativeLogicalTypeOccurrence retained;
        retained.kind = occurrence.kind;
        retained.source_ordinal = occurrence.source_ordinal;
        if (type_argument_mapping && retained.kind == RelativeLogicalTypeOccurrenceKind::TypeArgument)
            retained.source_ordinal = (*type_argument_mapping)[retained.source_ordinal].reference;
        retained.path.reserve(occurrence.path.size() + (prefix ? 1 : 0));
        retained.path.insert(retained.path.end(), occurrence.path.begin(), occurrence.path.end());
        /// Expansion proceeds leaf-to-root. Keep this temporary order so each
        /// added outer locator is O(1); finish() reverses every path exactly
        /// once before the public batch escapes.
        if (prefix)
            retained.path.push_back(*prefix);
        destination.push_back(std::move(retained));
    }
}

void TemplateSpecializer::Attempt::State::prefixAndMoveOccurrences(
    std::vector<RelativeLogicalTypeOccurrence> & destination,
    std::vector<RelativeLogicalTypeOccurrence> source,
    std::optional<PhysicalTypeChildLocator> prefix)
{
    if (source.size() > destination.max_size() - destination.size())
        fail(ErrorCode::LimitExceeded, "logical occurrence vector exceeds the host size domain");
    const UInt64 moved_occurrences = checkedSize(source.size(), "moved logical occurrence count does not fit UInt64");
    const UInt64 added_components = prefix ? moved_occurrences : 0;
    if (added_components > std::numeric_limits<UInt64>::max() - moved_occurrences)
        fail(ErrorCode::LimitExceeded, "moved logical occurrence work overflows UInt64");
    ensureProspective(statistics.charged_work, moved_occurrences + added_components, limits.maximum_work, "moved logical occurrence work");
    chargeQueryMemoScratch(
        checkedProduct(added_components, sizeof(PhysicalTypeChildLocator), "query specialization moved path retention overflows UInt64"));
    chargeQueryMemoScratch(
        checkedProduct(moved_occurrences, sizeof(RelativeLogicalTypeOccurrence), "query specialization moved occurrence vector overflows UInt64"));
    if (prefix)
    {
        addProspectively(
            statistics.retained_path_components, added_components, limits.maximum_retained_path_components, "retained path components");
        for (auto & occurrence : source)
        {
            if (occurrence.path.size() == occurrence.path.max_size())
                fail(ErrorCode::LimitExceeded, "logical path exceeds the host size domain");
            occurrence.path.push_back(*prefix);
        }
    }
    chargeWork(moved_occurrences + added_components);
    destination.reserve(destination.size() + source.size());
    destination.insert(destination.end(), std::make_move_iterator(source.begin()), std::make_move_iterator(source.end()));
}

Definition::Ptr TemplateSpecializer::Attempt::State::resolveDefinition(const DefinitionIdentity & identity, UInt32 * handle_index)
{
    if (!identityIsValid(identity) || identity.database_uuid != database_uuid)
        fail(ErrorCode::InvalidIdentity, "definition identity is invalid or crosses the database authority");
    if (const auto found = definition_handle_indexes.find(identity); found != definition_handle_indexes.end())
    {
        if (handle_index)
            *handle_index = found->second;
        return definition_handles[found->second];
    }

    /// Every absent identity would consume a distinct retained handle. Reject
    /// that prospective cost before charging or touching the authority.
    preflightDefinitionRetention();
    addProspectively(statistics.definition_lookups, 1, limits.maximum_definition_lookups, "definition lookups");
    chargeWork();
    Definition::Ptr definition;
    try
    {
        definition = session->findByIdentity(identity);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(ErrorCode::AuthorityFailure, fmt::format("definition lookup failed: {}", error.message()));
    }
    if (!definition)
        fail(ErrorCode::DefinitionNotFound, "definition identity is absent from the pinned authority generation");
    if (definition->getIdentity() != identity)
        fail(ErrorCode::AuthorityFailure, "authority returned a definition with the wrong immutable identity");
    validateDefinitionRetentionLimits(*definition);
    const UInt32 retained_index = retainDefinition(definition);
    if (handle_index)
        *handle_index = retained_index;
    return definition;
}

void TemplateSpecializer::Attempt::State::preflightDefinitionRetention() const
{
    const UInt64 retained_count = checkedSize(definition_handles.size(), "definition handle count does not fit UInt64");
    const UInt64 id_domain = std::numeric_limits<UInt32>::max();
    const UInt64 admission_limit = std::min(
        {capabilities.limits.maximum_definitions,
         capabilities.limits.maximum_transitive_dependencies,
         limits.maximum_definition_handles,
         id_domain});
    if (retained_count < admission_limit)
        return;
    if (retained_count >= capabilities.limits.maximum_definitions)
        fail(ErrorCode::LimitExceeded, "distinct definition handles exceed the authority definition-count limit");
    if (retained_count >= capabilities.limits.maximum_transitive_dependencies)
        fail(ErrorCode::LimitExceeded, "distinct definition handles exceed the authority traversal limit");
    if (retained_count >= limits.maximum_definition_handles)
        fail(ErrorCode::LimitExceeded, "distinct definition handles exceed their limit");
    if (retained_count >= id_domain)
        fail(ErrorCode::LimitExceeded, "distinct definition handles exceed the handle-ID domain");
    fail(ErrorCode::InvalidTemplate, "definition admission limit selection is inconsistent");
}

void TemplateSpecializer::Attempt::State::validateDefinitionRetentionLimits(const Definition & definition) const
{
    const auto & authority_limits = capabilities.limits;
    if (checkedSize(definition.getNodes().size(), "definition node count does not fit UInt64") > authority_limits.maximum_template_nodes)
        fail(ErrorCode::LimitExceeded, "definition template nodes exceed the authority limit");
    if (checkedSize(definition.getDependencies().size(), "definition dependency count does not fit UInt64")
        > authority_limits.maximum_direct_dependencies)
        fail(ErrorCode::LimitExceeded, "definition direct dependencies exceed the authority limit");
    if (definition.getCertificate().transitive_dependency_count > authority_limits.maximum_transitive_dependencies)
        fail(ErrorCode::LimitExceeded, "definition transitive dependencies exceed the authority limit");
    if (definition.getCertificate().charged_work > authority_limits.maximum_checker_work)
        fail(ErrorCode::LimitExceeded, "definition checker work exceeds the authority limit");
    if (!tryCountLogicalRetainedDefinitionBytes(definition, authority_limits.maximum_definition_bytes))
        fail(ErrorCode::LimitExceeded, "definition logical retained bytes exceed the authority limit");
}

UInt32 TemplateSpecializer::Attempt::State::retainDefinition(const Definition::Ptr & definition)
{
    const auto & identity = definition->getIdentity();
    if (const auto found = definition_handle_indexes.find(identity); found != definition_handle_indexes.end())
    {
        if (definition_handles[found->second].get() != definition.get())
            fail(ErrorCode::AuthorityFailure, "one pinned generation returned two objects for one immutable definition identity");
        return found->second;
    }

    preflightDefinitionRetention();
    chargeWork();
    chargeQueryMemoScratch(sizeof(Definition::Ptr) + sizeof(std::pair<const DefinitionIdentity, UInt32>));
    const auto index = static_cast<UInt32>(definition_handles.size());
    definition_handle_indexes.emplace(identity, index);
    definition_handles.push_back(definition);
    statistics.distinct_definition_handles = checkedSize(definition_handles.size(), "definition handle count does not fit UInt64");
    return index;
}

CanonicalTypeArguments
TemplateSpecializer::Attempt::State::validateArguments(const Definition & definition, const CanonicalTypeArguments & arguments) const
{
    const UInt64 encoded_size = checkedSize(arguments.encoded().size(), "canonical argument bytes do not fit UInt64");
    if (encoded_size > limits.maximum_canonical_argument_bytes)
        fail(ErrorCode::LimitExceeded, "canonical argument bytes exceed their specialization limit");
    try
    {
        return CanonicalTypeArguments::validate(
            definition.getParameters(),
            arguments.values(),
            limits.maximum_canonical_argument_bytes,
            limits.maximum_canonical_argument_item_bytes);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(ErrorCode::InvalidArguments, error.message());
    }
}

bool TemplateSpecializer::Attempt::State::argumentIsZero(const CanonicalTypeArgumentValue & argument)
{
    if (isUnsignedIntegerParameter(argument.kind) && std::holds_alternative<UInt64>(argument.value))
        return std::get<UInt64>(argument.value) == 0;
    if (isSignedIntegerParameter(argument.kind) && std::holds_alternative<Int64>(argument.value))
        return std::get<Int64>(argument.value) == 0;
    fail(ErrorCode::InvalidTemplate, "TYPE_IF_ZERO references a non-integer canonical argument");
}

CanonicalTypeArguments TemplateSpecializer::Attempt::State::decrementedArguments(
    const Definition & definition, const CanonicalTypeArguments & arguments, const TemplateNode & node)
{
    if (!capabilities.contains(TypeAuthorityCapability::DecreasingRecursion))
        fail(ErrorCode::MissingCapability, "authority does not advertise decreasing-recursion specialization");
    if (!definition.getDecreasingParameter() || node.parameter != *definition.getDecreasingParameter() || node.decrement != 1
        || node.parameter >= arguments.values().size())
        fail(ErrorCode::InvalidTemplate, "self call disagrees with its checked decreasing measure");
    chargeWork(
        checkedSize(arguments.encoded().size(), "self-call argument bytes do not fit UInt64")
        + checkedSize(arguments.values().size(), "self-call argument count does not fit UInt64"));
    std::vector<CanonicalTypeArgumentValue> values = arguments.values();
    auto & measure = values[node.parameter];
    if (!isUnsignedIntegerParameter(measure.kind) || !std::holds_alternative<UInt64>(measure.value))
        fail(ErrorCode::InvalidTemplate, "self-call measure is not an unsigned canonical argument");
    UInt64 & value = std::get<UInt64>(measure.value);
    if (value == 0)
        fail(ErrorCode::NonDecreasingRecursion, "self call would not strictly decrease a positive measure");
    --value;
    try
    {
        return CanonicalTypeArguments::validate(
            definition.getParameters(),
            std::move(values),
            limits.maximum_canonical_argument_bytes,
            limits.maximum_canonical_argument_item_bytes);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(ErrorCode::InvalidTemplate, error.message());
    }
}

CanonicalTypeArguments TemplateSpecializer::Attempt::State::dependencyArguments(
    const Definition & target, const CanonicalTypeArguments & caller_arguments, const TemplateNode & call)
{
    if (call.children.size() != target.getParameters().size())
        fail(ErrorCode::InvalidTemplate, "definition-call arity disagrees with the checked target definition");
    chargeWork(
        checkedSize(caller_arguments.encoded().size(), "definition-call argument bytes do not fit UInt64")
        + checkedSize(call.children.size(), "definition-call argument count does not fit UInt64"));
    std::vector<CanonicalTypeArgumentValue> values;
    values.reserve(call.children.size());
    for (std::size_t index = 0; index < call.children.size(); ++index)
    {
        const auto & edge = call.children[index];
        if (!edge.label.empty() || edge.reference >= caller_arguments.values().size())
            fail(ErrorCode::InvalidTemplate, "definition-call argument reference is invalid");
        const auto & value = caller_arguments.values()[edge.reference];
        if (value.kind != target.getParameters()[index].kind)
            fail(ErrorCode::InvalidTemplate, "definition-call argument kind disagrees with the checked target definition");
        values.push_back(value);
    }
    try
    {
        return CanonicalTypeArguments::validate(
            target.getParameters(),
            std::move(values),
            limits.maximum_canonical_argument_bytes,
            limits.maximum_canonical_argument_item_bytes);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(ErrorCode::InvalidTemplate, error.message());
    }
}

TemplateSpecializer::Attempt::State::ExpandedField TemplateSpecializer::Attempt::State::expandFieldValue(
    const Definition & definition,
    const CanonicalTypeArguments & arguments,
    TemplateNodeID node_id,
    UInt64 specialization_depth,
    UInt64 field_depth,
    bool charge_root)
{
    static_cast<void>(arguments);
    static_cast<void>(specialization_depth);
    if (field_depth > limits.maximum_field_depth)
        fail(ErrorCode::LimitExceeded, "canonical Field-value depth exceeds its limit");
    if (node_id >= definition.getNodes().size())
        fail(ErrorCode::InvalidTemplate, "canonical Field-value node reference is out of range");
    if (charge_root)
        chargeTemplateNode();
    const auto & node = definition.getNodes()[node_id];
    if (node.kind != TemplateNodeKind::FieldValue)
        fail(ErrorCode::InvalidTemplate, "canonical Field-value edge targets another node kind");

    const UInt64 child_count = checkedSize(node.children.size(), "canonical Field-value child count does not fit UInt64");
    ensureProspective(
        statistics.template_node_occurrences,
        child_count,
        limits.maximum_template_node_occurrences,
        "canonical Field-value children exceed the template-node limit");
    /// Charge the input-sized traversal before any composite reserve. Each
    /// child later charges its own node visit independently.
    chargeWork(child_count);

    const auto child = [&](std::size_t index)
    {
        if (index >= node.children.size())
            fail(ErrorCode::InvalidTemplate, "canonical Field-value child index is out of range");
        return expandFieldValue(definition, arguments, node.children[index].reference, specialization_depth, field_depth + 1, true);
    };

    switch (node.field_value.kind)
    {
        case CanonicalFieldKind::None: fail(ErrorCode::InvalidTemplate, "canonical Field-value kind is absent");
        case CanonicalFieldKind::Null: return {.field = Field{}};
        case CanonicalFieldKind::NegativeInfinity: return {.field = Field(NEGATIVE_INFINITY)};
        case CanonicalFieldKind::PositiveInfinity: return {.field = Field(POSITIVE_INFINITY)};
        case CanonicalFieldKind::UInt64:
            return {.field = Field(readLittleEndianExact<UInt64>(node.field_value.payload, "invalid UInt64 Field payload"))};
        case CanonicalFieldKind::Int64:
            return {.field = Field(readLittleEndianExact<Int64>(node.field_value.payload, "invalid Int64 Field payload"))};
        case CanonicalFieldKind::Float64:
            return {.field = Field(readLittleEndianExact<Float64>(node.field_value.payload, "invalid Float64 Field payload"))};
        case CanonicalFieldKind::String: chargeASTString(node.field_value.payload); return {.field = Field(node.field_value.payload)};
        case CanonicalFieldKind::Bool:
            if (node.field_value.payload.size() != 1 || static_cast<UInt8>(node.field_value.payload.front()) > 1)
                fail(ErrorCode::InvalidTemplate, "invalid Bool Field payload");
            return {.field = Field(static_cast<bool>(node.field_value.payload.front()))};
        case CanonicalFieldKind::UInt128:
            return {.field = Field(readLittleEndianExact<UInt128>(node.field_value.payload, "invalid UInt128 Field payload"))};
        case CanonicalFieldKind::Int128:
            return {.field = Field(readLittleEndianExact<Int128>(node.field_value.payload, "invalid Int128 Field payload"))};
        case CanonicalFieldKind::UInt256:
            return {.field = Field(readLittleEndianExact<UInt256>(node.field_value.payload, "invalid UInt256 Field payload"))};
        case CanonicalFieldKind::Int256:
            return {.field = Field(readLittleEndianExact<Int256>(node.field_value.payload, "invalid Int256 Field payload"))};
        case CanonicalFieldKind::Decimal32:
            return {.field = Field(readDecimalExact<Decimal32>(node.field_value.payload, "invalid Decimal32 Field payload"))};
        case CanonicalFieldKind::Decimal64:
            return {.field = Field(readDecimalExact<Decimal64>(node.field_value.payload, "invalid Decimal64 Field payload"))};
        case CanonicalFieldKind::Decimal128:
            return {.field = Field(readDecimalExact<Decimal128>(node.field_value.payload, "invalid Decimal128 Field payload"))};
        case CanonicalFieldKind::Decimal256:
            return {.field = Field(readDecimalExact<Decimal256>(node.field_value.payload, "invalid Decimal256 Field payload"))};
        case CanonicalFieldKind::UUID: {
            if (node.field_value.payload.size() != CanonicalUUID{}.size())
                fail(ErrorCode::InvalidTemplate, "invalid UUID Field payload");
            CanonicalUUID bytes{};
            std::copy(node.field_value.payload.begin(), node.field_value.payload.end(), bytes.begin());
            return {.field = Field(uuidFromCanonicalBytes(bytes))};
        }
        case CanonicalFieldKind::IPv4: {
            const auto value = readLittleEndianExact<IPv4::UnderlyingType>(node.field_value.payload, "invalid IPv4 Field payload");
            return {.field = Field(IPv4(value))};
        }
        case CanonicalFieldKind::IPv6: {
            const auto value = readLittleEndianExact<IPv6::UnderlyingType>(node.field_value.payload, "invalid IPv6 Field payload");
            return {.field = Field(IPv6(value))};
        }
        case CanonicalFieldKind::Array: {
            Array result;
            result.reserve(node.children.size());
            for (std::size_t index = 0; index < node.children.size(); ++index)
                result.push_back(std::move(child(index).field));
            return {.field = Field(std::move(result))};
        }
        case CanonicalFieldKind::Tuple: {
            Tuple result;
            result.reserve(node.children.size());
            for (std::size_t index = 0; index < node.children.size(); ++index)
                result.push_back(std::move(child(index).field));
            return {.field = Field(std::move(result))};
        }
        case CanonicalFieldKind::Map: {
            if ((node.children.size() % 2) != 0)
                fail(ErrorCode::InvalidTemplate, "canonical Map Field has odd key/value arity");
            Map result;
            result.reserve(node.children.size() / 2);
            for (std::size_t index = 0; index < node.children.size(); index += 2)
            {
                Tuple pair;
                pair.reserve(2);
                pair.push_back(std::move(child(index).field));
                pair.push_back(std::move(child(index + 1).field));
                result.emplace_back(std::move(pair));
            }
            return {.field = Field(std::move(result))};
        }
        case CanonicalFieldKind::Object: {
            Object result;
            std::string_view previous;
            bool has_previous = false;
            for (std::size_t index = 0; index < node.children.size(); ++index)
            {
                const auto & edge = node.children[index];
                if (has_previous && !binaryStringLess(previous, edge.label))
                    fail(ErrorCode::InvalidTemplate, "canonical Object Field keys are not in strict binary order");
                chargeASTString(edge.label);
                if (!result.emplace(edge.label, std::move(child(index).field)).second)
                    fail(ErrorCode::InvalidTemplate, "canonical Object Field contains a duplicate key");
                previous = edge.label;
                has_previous = true;
            }
            return {.field = Field(std::move(result))};
        }
        case CanonicalFieldKind::AggregateFunctionState:
            chargeASTString(node.field_value.name);
            chargeASTString(node.field_value.payload);
            return {.field = Field(AggregateFunctionStateData{.name = node.field_value.name, .data = node.field_value.payload})};
    }
    fail(ErrorCode::InvalidTemplate, "canonical Field-value kind is unknown");
}

ExpandedNode TemplateSpecializer::Attempt::State::expandNode(
    const Definition & definition,
    const CanonicalTypeArguments & arguments,
    TemplateNodeID node_id,
    UInt64 specialization_depth,
    UInt64 ast_depth)
{
    if (node_id >= definition.getNodes().size())
        fail(ErrorCode::InvalidTemplate, "template node reference is out of range");
    checkASTDepth(ast_depth);
    chargeTemplateNode();
    const auto & node = definition.getNodes()[node_id];

    const auto leaf = [&](ASTPtr ast) -> ExpandedNode
    { return {.ast = std::move(ast), .cost = {.nodes = 1, .edges = 0, .depth = 1}, .occurrences = {}}; };
    const auto literalArgument = [&](const CanonicalTypeArgumentValue & actual) -> ExpandedNode
    {
        switch (actual.kind)
        {
            case ParameterKind::Type: {
                if (!std::holds_alternative<CanonicalTypeArgument>(actual.value))
                    fail(ErrorCode::InvalidArguments, "TYPE actual has an invalid representation");
                const auto & type = std::get<CanonicalTypeArgument>(actual.value);
                const auto cost = inspectCanonicalAST(type.getCanonicalASTForSpecialization(), 1);
                if (cost.depth > limits.maximum_ast_depth - ast_depth + 1)
                    fail(ErrorCode::LimitExceeded, "substituted TYPE argument exceeds the AST-depth limit");
                return {.ast = type.getCanonicalASTForSpecialization(), .cost = cost, .occurrences = {}};
            }
            case ParameterKind::Bool:
                if (!std::holds_alternative<bool>(actual.value))
                    fail(ErrorCode::InvalidArguments, "Bool actual has an invalid representation");
                return leaf(makeLiteralAST(Field(std::get<bool>(actual.value))));
            case ParameterKind::UInt8:
            case ParameterKind::UInt16:
            case ParameterKind::UInt32:
            case ParameterKind::UInt64:
                if (!std::holds_alternative<UInt64>(actual.value))
                    fail(ErrorCode::InvalidArguments, "unsigned actual has an invalid representation");
                return leaf(makeLiteralAST(Field(std::get<UInt64>(actual.value))));
            case ParameterKind::Int8:
            case ParameterKind::Int16:
            case ParameterKind::Int32:
            case ParameterKind::Int64:
                if (!std::holds_alternative<Int64>(actual.value))
                    fail(ErrorCode::InvalidArguments, "signed actual has an invalid representation");
                return leaf(makeLiteralAST(Field(std::get<Int64>(actual.value))));
            case ParameterKind::String:
                if (!std::holds_alternative<String>(actual.value))
                    fail(ErrorCode::InvalidArguments, "String actual has an invalid representation");
                chargeASTString(std::get<String>(actual.value));
                return leaf(makeLiteralAST(Field(std::get<String>(actual.value))));
        }
        fail(ErrorCode::InvalidArguments, "canonical actual kind is unknown");
    };

    switch (node.kind)
    {
        case TemplateNodeKind::BuiltIn: return expandBuiltIn(definition, arguments, node, specialization_depth, ast_depth);
        case TemplateNodeKind::TypeParameter: {
            if (node.parameter >= arguments.values().size())
                fail(ErrorCode::InvalidTemplate, "template parameter reference is out of range");
            if (arguments.values()[node.parameter].kind != ParameterKind::Type)
                fail(ErrorCode::InvalidTemplate, "template parameter node kind disagrees with its formal");
            auto result = literalArgument(arguments.values()[node.parameter]);
            RelativeLogicalTypeOccurrence substitution{
                .path = {},
                .kind = RelativeLogicalTypeOccurrenceKind::TypeArgument,
                .source_ordinal = node.parameter,
            };
            chargeRetainedOccurrence(substitution);
            result.occurrences.push_back(std::move(substitution));
            return result;
        }
        case TemplateNodeKind::ValueParameter:
            if (node.parameter >= arguments.values().size())
                fail(ErrorCode::InvalidTemplate, "template parameter reference is out of range");
            if (arguments.values()[node.parameter].kind == ParameterKind::Type)
                fail(ErrorCode::InvalidTemplate, "template parameter node kind disagrees with its formal");
            return literalArgument(arguments.values()[node.parameter]);
        case TemplateNodeKind::UnsignedLiteral: return leaf(makeLiteralAST(Field(node.unsigned_literal)));
        case TemplateNodeKind::BooleanLiteral: return leaf(makeLiteralAST(Field(node.boolean_literal)));
        case TemplateNodeKind::SignedLiteral: return leaf(makeLiteralAST(Field(node.signed_literal)));
        case TemplateNodeKind::StringLiteral: chargeASTString(node.text); return leaf(makeLiteralAST(Field(node.text)));
        case TemplateNodeKind::Identifier: return leaf(makeIdentifierAST(node.text));
        case TemplateNodeKind::SpecializedEnum: {
            const UInt64 entry_count = checkedSize(node.enum_entries.size(), "specialized Enum entry count does not fit UInt64");
            chargeEnumEntries(entry_count);
            chargeASTString(node.specialized_enum_width == SpecializedEnumWidth::Enum8 ? "Enum8" : "Enum16");
            for (const auto & entry : node.enum_entries)
                chargeASTString(entry.name);
            auto ast = makeASTNode<ASTEnumDataType>();
            ast->name = node.specialized_enum_width == SpecializedEnumWidth::Enum8 ? "Enum8" : "Enum16";
            ast->values.reserve(node.enum_entries.size());
            for (const auto & entry : node.enum_entries)
                ast->values.emplace_back(entry.name, entry.value);
            return leaf(std::move(ast));
        }
        case TemplateNodeKind::FieldValue: {
            auto expanded = expandFieldValue(definition, arguments, node_id, specialization_depth, 1, false);
            return leaf(makeLiteralAST(std::move(expanded.field)));
        }
        case TemplateNodeKind::AggregateFunction: {
            const UInt64 parameter_count = checkedSize(node.children.size(), "aggregate-function parameter count does not fit UInt64");
            ensureProspective(
                statistics.template_node_occurrences,
                parameter_count,
                limits.maximum_template_node_occurrences,
                "aggregate-function parameters exceed the template-node limit");
            chargeWork(parameter_count);
            if (parameter_count != 0 || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
            {
                ensureProspective(
                    statistics.constructed_ast_nodes,
                    2,
                    limits.maximum_constructed_ast_nodes,
                    "aggregate-function AST exceeds the constructed-node limit");
                ensureProspective(
                    statistics.constructed_ast_edges,
                    1 + parameter_count,
                    limits.maximum_constructed_ast_edges,
                    "aggregate-function AST exceeds the constructed-edge limit");
            }
            std::vector<ASTPtr> parameter_asts;
            parameter_asts.reserve(node.children.size());
            std::vector<PhysicalASTCost> costs;
            costs.reserve(node.children.size());
            for (const auto & edge : node.children)
            {
                auto expanded
                    = expandNode(definition, arguments, edge.reference, specialization_depth, ast_depth + (node.children.empty() ? 0 : 2));
                if (!expanded.occurrences.empty())
                    fail(ErrorCode::InvalidTemplate, "aggregate-function Field parameter contains a logical type occurrence");
                parameter_asts.push_back(std::move(expanded.ast));
                costs.push_back(expanded.cost);
            }
            if (parameter_asts.empty() && node.aggregate_nulls_action == AggregateFunctionNullsAction::Empty)
                return leaf(makeIdentifierAST(node.text));
            auto function = makeFunctionAST(node.text, std::move(parameter_asts), toParserNullsAction(node.aggregate_nulls_action));
            const auto cost = composeCost(2, 1 + checkedSize(costs.size(), "aggregate parameter count does not fit UInt64"), 2, 2, costs);
            checkASTDepth(ast_depth + cost.depth - 1);
            return {.ast = std::move(function), .cost = cost, .occurrences = {}};
        }
        case TemplateNodeKind::DynamicSetting:
        case TemplateNodeKind::ObjectSetting: {
            if (node.children.size() != 1)
                fail(ErrorCode::InvalidTemplate, "named setting does not have exactly one value");
            auto value = expandNode(definition, arguments, node.children.front().reference, specialization_depth, ast_depth + 2);
            if (!value.occurrences.empty())
                fail(ErrorCode::InvalidTemplate, "named setting contains a logical type occurrence");
            auto identifier = makeIdentifierAST(node.text);
            const std::array<PhysicalASTCost, 2> costs{{{.nodes = 1, .edges = 0, .depth = 1}, value.cost}};
            std::vector<ASTPtr> asts{identifier, value.ast};
            auto function = makeFunctionAST("equals", std::move(asts));
            const auto cost = composeCost(2, 3, 2, 2, costs);
            if (node.kind == TemplateNodeKind::DynamicSetting)
            {
                checkASTDepth(ast_depth + cost.depth - 1);
                return {.ast = std::move(function), .cost = cost, .occurrences = {}};
            }

            chargeConstructedNodes(1);
            chargeConstructedEdges(1);
            auto wrapper = make_intrusive<ASTObjectTypeArgument>();
            wrapper->parameter = function;
            wrapper->children.push_back(std::move(function));
            const std::array<PhysicalASTCost, 1> wrapper_child{cost};
            const auto wrapper_cost = composeCost(1, 1, 1, 1, wrapper_child);
            checkASTDepth(ast_depth + wrapper_cost.depth - 1);
            return {.ast = std::move(wrapper), .cost = wrapper_cost, .occurrences = {}};
        }
        case TemplateNodeKind::ObjectTypedPath: {
            if (node.children.size() != 1)
                fail(ErrorCode::InvalidTemplate, "typed Object path does not have exactly one type");
            auto type = expandNode(definition, arguments, node.children.front().reference, specialization_depth, ast_depth + 2);
            chargeASTString(node.text);
            chargeConstructedNodes(2);
            chargeConstructedEdges(2);
            auto typed = make_intrusive<ASTObjectTypedPathArgument>();
            typed->path = node.text;
            typed->type = type.ast;
            typed->children.push_back(type.ast);
            auto wrapper = make_intrusive<ASTObjectTypeArgument>();
            wrapper->path_with_type = typed;
            wrapper->children.push_back(std::move(typed));
            const std::array<PhysicalASTCost, 1> costs{type.cost};
            const auto cost = composeCost(2, 2, 2, 2, costs);
            checkASTDepth(ast_depth + cost.depth - 1);
            return {.ast = std::move(wrapper), .cost = cost, .occurrences = std::move(type.occurrences)};
        }
        case TemplateNodeKind::ObjectSkipPath: {
            auto identifier = makeIdentifierAST(node.text);
            chargeConstructedNodes(1);
            chargeConstructedEdges(1);
            auto wrapper = make_intrusive<ASTObjectTypeArgument>();
            wrapper->skip_path = identifier;
            wrapper->children.push_back(std::move(identifier));
            return {.ast = std::move(wrapper), .cost = {.nodes = 2, .edges = 1, .depth = 2}, .occurrences = {}};
        }
        case TemplateNodeKind::ObjectSkipRegexp: {
            chargeASTString(node.text);
            auto literal = makeLiteralAST(Field(node.text));
            chargeConstructedNodes(1);
            chargeConstructedEdges(1);
            auto wrapper = make_intrusive<ASTObjectTypeArgument>();
            wrapper->skip_path_regexp = literal;
            wrapper->children.push_back(std::move(literal));
            return {.ast = std::move(wrapper), .cost = {.nodes = 2, .edges = 1, .depth = 2}, .occurrences = {}};
        }
        case TemplateNodeKind::TypeIfZero: {
            if (node.parameter >= arguments.values().size() || node.children.size() != 2)
                fail(ErrorCode::InvalidTemplate, "TYPE_IF_ZERO has an invalid parameter or branch count");
            const auto selected = argumentIsZero(arguments.values()[node.parameter]) ? 0 : 1;
            return expandNode(definition, arguments, node.children[selected].reference, specialization_depth, ast_depth);
        }
        case TemplateNodeKind::SelfCall: {
            const auto next_arguments = decrementedArguments(definition, arguments, node);
            const auto id = specializeDefinition(definition.getIdentity(), next_arguments, specialization_depth + 1);
            const auto & entry = *entries[id];
            if (entry.state != MemoState::Complete)
                fail(ErrorCode::ActiveCycle, "self call reached an active specialization");
            chargeEmitted(entry);
            ExpandedNode result{.ast = entry.ast, .cost = entry.cost, .occurrences = {}};
            appendOccurrences(result.occurrences, entry.occurrences, std::nullopt);
            return result;
        }
        case TemplateNodeKind::DefinitionCall: {
            if (node.dependency_ordinal >= definition.getDependencies().size())
                fail(ErrorCode::InvalidTemplate, "definition-call dependency ordinal is out of range");
            const auto & dependency = definition.getDependencies()[node.dependency_ordinal];
            const DefinitionIdentity target_identity{
                .database_uuid = definition.getIdentity().database_uuid,
                .type_uuid = dependency.type_uuid,
                .revision = dependency.revision};
            const auto target = resolveDefinition(target_identity);
            if (target->getDefinitionHash() != dependency.target_definition_hash)
                fail(ErrorCode::DependencyMismatch, "definition-call target hash disagrees with the checked dependency");
            const auto target_arguments = dependencyArguments(*target, arguments, node);
            const auto id = specializeDefinition(target_identity, target_arguments, specialization_depth + 1);
            const auto & entry = *entries[id];
            if (entry.state != MemoState::Complete)
                fail(ErrorCode::ActiveCycle, "definition call reached an active specialization");
            chargeEmitted(entry);
            ExpandedNode result{.ast = entry.ast, .cost = entry.cost, .occurrences = {}};
            appendOccurrences(result.occurrences, entry.occurrences, std::nullopt, &node.children);
            return result;
        }
    }
    fail(ErrorCode::InvalidTemplate, "template node kind is unknown");
}

ExpandedNode TemplateSpecializer::Attempt::State::expandBuiltIn(
    const Definition & definition,
    const CanonicalTypeArguments & arguments,
    const TemplateNode & node,
    UInt64 specialization_depth,
    UInt64 ast_depth)
{
    const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(node.atom);
    if (!classification)
        fail(ErrorCode::InvalidTemplate, "checked built-in family is no longer registered");
    if (classification.input_class == BuiltInDataTypeCreatorInputClass::CanonicalizeGenericEnumArguments)
        fail(ErrorCode::InvalidTemplate, "generic Enum template input must have been canonicalized as a specialized Enum node");
    const std::string_view family = classification.family->canonical_creator_name;

    const UInt64 child_count = checkedSize(node.children.size(), "built-in child count does not fit UInt64");
    ensureProspective(
        statistics.template_node_occurrences,
        child_count,
        limits.maximum_template_node_occurrences,
        "built-in children exceed the template-node limit");
    /// Pay for the input-sized traversal before allocating its scratch arrays.
    /// Every expanded child later charges its own template-node visit too.
    chargeWork(child_count);
    if (child_count != 0)
    {
        const UInt64 minimum_nodes = family == "Nested" ? 2 + child_count : 2;
        const UInt64 minimum_edges = family == "Nested" ? 1 + 2 * child_count : 1 + child_count;
        ensureProspective(
            statistics.constructed_ast_nodes,
            minimum_nodes,
            limits.maximum_constructed_ast_nodes,
            "built-in output exceeds the constructed-node limit");
        ensureProspective(
            statistics.constructed_ast_edges,
            minimum_edges,
            limits.maximum_constructed_ast_edges,
            "built-in output exceeds the constructed-edge limit");
    }

    if (family == "JSON")
    {
        enum class ObjectArgumentGroup : UInt8
        {
            MaxDynamicTypesSetting,
            MaxDynamicPathsSetting,
            TypedPath,
            SkipPath,
            SkipRegexp,
        };
        const auto group = [&](const TemplateNode & argument)
        {
            switch (argument.kind)
            {
                case TemplateNodeKind::ObjectSetting:
                    if (argument.text == "max_dynamic_types")
                        return ObjectArgumentGroup::MaxDynamicTypesSetting;
                    if (argument.text == "max_dynamic_paths")
                        return ObjectArgumentGroup::MaxDynamicPathsSetting;
                    fail(ErrorCode::InvalidTemplate, "JSON/Object child has an unknown setting name");
                case TemplateNodeKind::ObjectTypedPath: return ObjectArgumentGroup::TypedPath;
                case TemplateNodeKind::ObjectSkipPath: return ObjectArgumentGroup::SkipPath;
                case TemplateNodeKind::ObjectSkipRegexp: return ObjectArgumentGroup::SkipRegexp;
                default: fail(ErrorCode::InvalidTemplate, "JSON/Object child has the wrong parser-surface kind");
            }
        };
        std::optional<ObjectArgumentGroup> previous_group;
        std::string_view previous_key;
        for (const auto & edge : node.children)
        {
            if (edge.reference >= definition.getNodes().size())
                fail(ErrorCode::InvalidTemplate, "JSON/Object child reference is out of range");
            const auto & argument = definition.getNodes()[edge.reference];
            const auto current_group = group(argument);
            if (previous_group && current_group < *previous_group)
                fail(ErrorCode::InvalidTemplate, "JSON/Object arguments are outside canonical factory order");
            if (previous_group && current_group == *previous_group)
            {
                const bool ordered = binaryStringLess(previous_key, argument.text)
                    || (current_group == ObjectArgumentGroup::SkipRegexp && previous_key == argument.text);
                if (!ordered)
                    fail(ErrorCode::InvalidTemplate, "JSON/Object arguments are outside canonical factory order");
            }
            previous_group = current_group;
            previous_key = argument.text;
        }
    }

    std::vector<BuiltInChild> children;
    children.reserve(node.children.size());
    for (const auto & edge : node.children)
    {
        if (edge.reference >= definition.getNodes().size())
            fail(ErrorCode::InvalidTemplate, "built-in child reference is out of range");
        const auto & source = definition.getNodes()[edge.reference];
        const UInt64 child_ast_depth = ast_depth + (family == "Nested" ? 3 : 2);
        BuiltInChild child{
            .kind = source.kind,
            .label = edge.label,
            .expansion = expandNode(definition, arguments, edge.reference, specialization_depth, child_ast_depth)};
        children.push_back(std::move(child));
    }

    const auto requireNoLabel = [&](const BuiltInChild & child)
    {
        if (!child.label.empty())
            fail(ErrorCode::InvalidTemplate, "only Tuple/Nested physical type children may carry field labels");
    };
    const auto addOccurrences = [&](ExpandedNode & output, BuiltInChild & child, PhysicalTypeChildLocator locator)
    { prefixAndMoveOccurrences(output.occurrences, std::move(child.expansion.occurrences), locator); };
    const auto rejectLogical = [&](const BuiltInChild & child)
    {
        if (!child.expansion.occurrences.empty())
            fail(ErrorCode::InvalidTemplate, "logical type occurrence is attached to a non-type built-in argument");
    };

    ExpandedNode output;
    std::vector<ASTPtr> ast_arguments;
    std::vector<PhysicalASTCost> argument_costs;
    ast_arguments.reserve(children.size());
    argument_costs.reserve(children.size());

    if (family == "Tuple")
    {
        bool has_labels = false;
        bool has_unlabelled = false;
        for (const auto & child : children)
        {
            if (!isTypeProducingNode(child.kind))
                fail(ErrorCode::InvalidTemplate, "Tuple child does not produce a type");
            has_labels |= !child.label.empty();
            has_unlabelled |= child.label.empty();
        }
        if (has_labels && has_unlabelled)
            fail(ErrorCode::InvalidTemplate, "Tuple mixes named and unnamed elements");
        for (std::size_t index = 0; index < children.size(); ++index)
        {
            auto & child = children[index];
            addOccurrences(
                output, child, {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = static_cast<UInt32>(index)});
            ast_arguments.push_back(child.expansion.ast);
            argument_costs.push_back(child.expansion.cost);
        }
        chargeASTString(family);
        if (children.empty())
        {
            auto tuple = makeASTNode<ASTTupleDataType>();
            tuple->name = String(family);
            output.ast = std::move(tuple);
            output.cost = {.nodes = 1, .edges = 0, .depth = 1};
            return output;
        }
        if (has_labels)
            for (const auto & child : children)
                chargeASTString(child.label);
        chargeConstructedNodes(2);
        chargeConstructedEdges(1 + checkedSize(ast_arguments.size(), "Tuple argument count does not fit UInt64"));
        auto tuple = make_intrusive<ASTTupleDataType>();
        tuple->name = String(family);
        auto list = make_intrusive<ASTExpressionList>();
        list->children.reserve(ast_arguments.size());
        list->children.insert(
            list->children.end(), std::make_move_iterator(ast_arguments.begin()), std::make_move_iterator(ast_arguments.end()));
        tuple->children.push_back(std::move(list));
        if (has_labels)
        {
            tuple->element_names.reserve(children.size());
            for (const auto & child : children)
                tuple->element_names.emplace_back(child.label);
        }
        output.ast = std::move(tuple);
        output.cost
            = composeCost(2, 1 + checkedSize(argument_costs.size(), "Tuple argument count does not fit UInt64"), 2, 2, argument_costs);
        checkASTDepth(ast_depth + output.cost.depth - 1);
        return output;
    }

    if (family == "Nested")
    {
        absl::flat_hash_map<std::string_view, UInt8> names;
        names.reserve(children.size());
        for (std::size_t index = 0; index < children.size(); ++index)
        {
            auto & child = children[index];
            if (!isTypeProducingNode(child.kind) || child.label.empty() || !names.emplace(child.label, 0).second)
                fail(ErrorCode::InvalidTemplate, "Nested requires unique named type elements");
            addOccurrences(
                output, child, {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = static_cast<UInt32>(index)});
            chargeASTString(child.label);
            chargeConstructedNodes(1);
            chargeConstructedEdges(1);
            auto pair = make_intrusive<ASTNameTypePair>();
            pair->name = child.label;
            pair->type = child.expansion.ast;
            pair->children.push_back(child.expansion.ast);
            ast_arguments.push_back(std::move(pair));
            const std::array<PhysicalASTCost, 1> cost{child.expansion.cost};
            argument_costs.push_back(composeCost(1, 1, 1, 1, cost));
        }
    }
    else
    {
        UInt32 physical_type_ordinal = 0;
        std::size_t aggregate_function_index = std::numeric_limits<std::size_t>::max();
        if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
        {
            aggregate_function_index = 0;
            if (family == "AggregateFunction" && children.size() > 1)
            {
                const auto first_kind = children.front().kind;
                if (first_kind == TemplateNodeKind::UnsignedLiteral
                    || (first_kind == TemplateNodeKind::ValueParameter
                        && definition.getNodes()[node.children.front().reference].parameter < arguments.values().size()
                        && isUnsignedIntegerParameter(
                            arguments.values()[definition.getNodes()[node.children.front().reference].parameter].kind)))
                    aggregate_function_index = 1;
            }
        }

        UInt32 json_typed_path_ordinal = 0;
        for (std::size_t index = 0; index < children.size(); ++index)
        {
            auto & child = children[index];
            requireNoLabel(child);
            std::optional<PhysicalTypeChildLocator> locator;
            const bool produces_type = isTypeProducingNode(child.kind);

            if (family == "Array" || family == "Nullable" || family == "LowCardinality")
            {
                if (produces_type && index == 0)
                    locator = {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = 0};
                else if (produces_type)
                    fail(ErrorCode::InvalidTemplate, "unary physical type has an extra type argument");
            }
            else if (family == "Map")
            {
                if (produces_type && index < 2)
                    locator = {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = static_cast<UInt32>(index)};
                else if (produces_type)
                    fail(ErrorCode::InvalidTemplate, "Map has an extra type argument");
            }
            else if (family == "Variant")
            {
                if (!produces_type)
                    fail(ErrorCode::InvalidTemplate, "Variant child does not produce a type");
                locator = {.kind = PhysicalTypeChildLocatorKind::VariantNormalizedBranch, .source_ordinal = static_cast<UInt32>(index)};
            }
            else if (family == "QBit")
            {
                if (produces_type && index == 0)
                    locator = {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = 0};
                else if (produces_type)
                    fail(ErrorCode::InvalidTemplate, "QBit has an extra type argument");
            }
            else if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
            {
                if (index > aggregate_function_index)
                {
                    if (!produces_type)
                        fail(ErrorCode::InvalidTemplate, "aggregate-function argument does not produce a type");
                    locator = {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = physical_type_ordinal++};
                }
                else if (produces_type)
                    fail(ErrorCode::InvalidTemplate, "aggregate-function version/name position contains a type");
            }
            else if (family == "JSON")
            {
                if (child.kind == TemplateNodeKind::ObjectTypedPath)
                {
                    locator = {.kind = PhysicalTypeChildLocatorKind::StableOrdinal, .source_ordinal = json_typed_path_ordinal++};
                }
                else if (produces_type)
                    fail(ErrorCode::InvalidTemplate, "JSON/Object non-path argument directly produces a type");
            }
            else if (produces_type)
            {
                fail(ErrorCode::InvalidTemplate, "built-in family has no stable direct binary child for a logical type argument");
            }

            if (locator)
                addOccurrences(output, child, *locator);
            else
                rejectLogical(child);
            ast_arguments.push_back(child.expansion.ast);
            argument_costs.push_back(child.expansion.cost);
        }
    }

    chargeASTString(family);
    if (ast_arguments.empty())
    {
        auto type = makeASTNode<ASTDataType>();
        type->name = String(family);
        output.ast = std::move(type);
        output.cost = {.nodes = 1, .edges = 0, .depth = 1};
        return output;
    }

    chargeConstructedNodes(2);
    chargeConstructedEdges(1 + checkedSize(ast_arguments.size(), "built-in argument count does not fit UInt64"));
    auto type = make_intrusive<ASTDataType>();
    type->name = String(family);
    auto list = make_intrusive<ASTExpressionList>();
    list->children.reserve(ast_arguments.size());
    list->children.insert(
        list->children.end(), std::make_move_iterator(ast_arguments.begin()), std::make_move_iterator(ast_arguments.end()));
    type->children.push_back(std::move(list));
    output.ast = std::move(type);
    output.cost
        = composeCost(2, 1 + checkedSize(argument_costs.size(), "built-in argument count does not fit UInt64"), 2, 2, argument_costs);
    checkASTDepth(ast_depth + output.cost.depth - 1);
    return output;
}

TemplateSpecializationID TemplateSpecializer::Attempt::State::specializeDefinition(
    const DefinitionIdentity & identity,
    const CanonicalTypeArguments & arguments,
    UInt64 specialization_depth,
    const Digest * precomputed_arguments_digest)
{
    addProspectively(statistics.specialization_requests, 1, limits.maximum_work, "specialization requests");
    chargeWork();
    if (specialization_depth == 0 || specialization_depth > limits.maximum_specialization_depth)
        fail(ErrorCode::LimitExceeded, "specialization depth exceeds its limit");
    statistics.maximum_specialization_depth = std::max(statistics.maximum_specialization_depth, specialization_depth);

    UInt32 definition_handle_index = 0;
    const auto definition = resolveDefinition(identity, &definition_handle_index);
    const UInt64 input_argument_bytes = checkedSize(arguments.encoded().size(), "canonical argument bytes do not fit UInt64");
    if (input_argument_bytes > limits.maximum_canonical_argument_bytes)
        fail(ErrorCode::LimitExceeded, "canonical argument bytes exceed their specialization limit");
    const UInt64 input_argument_count = checkedSize(arguments.values().size(), "canonical argument count does not fit UInt64");
    /// Validation/canonical re-encoding and the memo digest are both linear in
    /// the caller-owned canonical bytes. An exact query-memo probe has already
    /// charged and computed the digest, so its miss charges only the remaining
    /// validation pass before the first input-sized argument copy.
    const UInt64 argument_passes = precomputed_arguments_digest ? 1 : 2;
    chargeWork(argument_passes * input_argument_bytes + input_argument_count);
    const Digest arguments_digest = precomputed_arguments_digest
        ? *precomputed_arguments_digest
        : hashDomainSeparated("ClickHouse UDT specialization memo key V1", arguments.encoded());
    const MemoKeyView lookup_key{identity, arguments_digest, arguments.encoded()};
    if (const auto found = memo.find(lookup_key); found != memo.end())
    {
        MemoEntry & entry = *entries[found->second];
        if (entry.state == MemoState::Active)
            fail(ErrorCode::ActiveCycle, "specialization memo encountered an active identical key");
        ++statistics.specialization_memo_hits;
        return found->second;
    }

    /// The miss will retain its canonical argument copy. Admit its complete
    /// key footprint before validate() allocates that copy or its encoding.
    chargeQueryMemoKey(arguments);
    CanonicalTypeArguments canonical_arguments = validateArguments(*definition, arguments);
    if (canonical_arguments.encoded() != arguments.encoded())
        fail(ErrorCode::InvalidArguments, "specialization input is not the canonical argument encoding for its definition");

    if (entries.size() >= limits.maximum_distinct_specializations
        || entries.size() >= static_cast<std::size_t>(invalid_template_specialization_id))
        fail(ErrorCode::LimitExceeded, "distinct specializations exceed their limit");
    const UInt64 argument_bytes = checkedSize(canonical_arguments.encoded().size(), "memo argument bytes do not fit UInt64");
    if (argument_bytes > std::numeric_limits<UInt64>::max() - memo_identity_bytes)
        fail(ErrorCode::LimitExceeded, "memo key byte count overflows UInt64");
    addProspectively(statistics.memo_key_bytes, memo_identity_bytes + argument_bytes, limits.maximum_memo_key_bytes, "memo key bytes");
    chargeWork();

    if (query_budget)
    {
        const auto admission = query_budget->chargeDistinctDescriptor(identity, canonical_arguments.encoded());
        if (!admission.isAccepted())
            fail(ErrorCode::LimitExceeded, formatResourceAdmissionFailure(admission));
    }

    const auto id = static_cast<TemplateSpecializationID>(entries.size());
    entries.push_back(std::make_unique<MemoEntry>(id, identity, definition_handle_index, std::move(canonical_arguments)));
    MemoEntry & entry = *entries.back();
    const auto [inserted, was_inserted] = memo.emplace(MemoKey{identity, arguments_digest, entry.arguments.encoded()}, id);
    static_cast<void>(inserted);
    if (!was_inserted)
        fail(ErrorCode::AuthorityFailure, "specialization memo insertion disagrees with its prior lookup");
    statistics.distinct_specializations = checkedSize(entries.size(), "specialization count does not fit UInt64");

    auto expanded = expandNode(*definition, entry.arguments, definition->getRoot(), specialization_depth, 1);
    if (!expanded.ast)
        fail(ErrorCode::InvalidTemplate, "template specialization produced a null physical AST");
    checkASTDepth(expanded.cost.depth);
    entry.ast = std::move(expanded.ast);
    entry.cost = expanded.cost;
    if (expanded.occurrences.size() == expanded.occurrences.max_size())
        fail(ErrorCode::LimitExceeded, "logical occurrence vector exceeds the host size domain");
    const UInt64 moved_occurrences = checkedSize(expanded.occurrences.size(), "specialization occurrence count does not fit UInt64");
    ensureProspective(statistics.retained_occurrences, 1, limits.maximum_retained_occurrences, "retained logical occurrences");
    if (moved_occurrences == std::numeric_limits<UInt64>::max())
        fail(ErrorCode::LimitExceeded, "specialization occurrence work overflows UInt64");
    ensureProspective(statistics.charged_work, moved_occurrences + 1, limits.maximum_work, "specialization occurrence work");
    chargeWork(moved_occurrences);
    RelativeLogicalTypeOccurrence root_occurrence{
        .path = {},
        .kind = RelativeLogicalTypeOccurrenceKind::Specialization,
        .source_ordinal = id,
    };
    chargeRetainedOccurrence(root_occurrence);
    chargeQueryMemoScratch(
        checkedProduct(
            moved_occurrences + 1,
            sizeof(RelativeLogicalTypeOccurrence),
            "query specialization result occurrence vector overflows UInt64"));
    entry.occurrences.reserve(expanded.occurrences.size() + 1);
    entry.occurrences.push_back(std::move(root_occurrence));
    entry.occurrences.insert(
        entry.occurrences.end(),
        std::make_move_iterator(expanded.occurrences.begin()),
        std::make_move_iterator(expanded.occurrences.end()));
    entry.state = MemoState::Complete;
    return id;
}

TemplateSpecializationID
TemplateSpecializer::Attempt::State::specializeFromQueryMemo(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
{
    if (!query_memo_retention_enabled)
        fail(ErrorCode::InvalidAttemptState, "exact query memo lookup is not enabled for this specialization state");
    if (!identityIsValid(identity) || identity.database_uuid != database_uuid)
        fail(ErrorCode::InvalidIdentity, "definition identity is invalid or crosses the query memo authority");

    const UInt64 encoded_size = checkedSize(arguments.encoded().size(), "query memo argument bytes do not fit UInt64");
    if (encoded_size > limits.maximum_canonical_argument_bytes)
        fail(ErrorCode::LimitExceeded, "canonical argument bytes exceed their query memo limit");
    chargeWork(encoded_size + 1);
    const Digest arguments_digest = hashDomainSeparated("ClickHouse UDT specialization memo key V1", arguments.encoded());
    const MemoKeyView lookup_key{identity, arguments_digest, arguments.encoded()};
    if (const auto found = memo.find(lookup_key); found != memo.end())
    {
        MemoEntry & entry = *entries[found->second];
        if (entry.state != MemoState::Complete)
            fail(ErrorCode::ActiveCycle, "query specialization memo encountered an active identical key");
        addProspectively(statistics.specialization_requests, 1, limits.maximum_work, "specialization requests");
        ++statistics.specialization_memo_hits;
        chargeEmitted(entry);
        return found->second;
    }
    const auto id = specializeDefinition(identity, arguments, 1, std::addressof(arguments_digest));
    const MemoEntry & entry = *entries[id];
    if (entry.state != MemoState::Complete)
        fail(ErrorCode::ActiveCycle, "query memo specialization remained active");
    chargeEmitted(entry);
    return id;
}

TemplateSpecializationID
TemplateSpecializer::Attempt::State::specialize(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
{
    const auto id = specializeDefinition(identity, arguments, 1);
    const MemoEntry & entry = *entries[id];
    if (entry.state != MemoState::Complete)
        fail(ErrorCode::ActiveCycle, "top-level specialization remained active");
    chargeEmitted(entry);
    return id;
}

TemplateSpecializationID TemplateSpecializer::Attempt::State::specializeEncoded(
    const DefinitionIdentity & identity, std::string_view canonical_arguments, const CanonicalTypeArgumentLimits & type_argument_limits)
{
    const auto definition = resolveDefinition(identity);
    const UInt64 encoded_size = checkedSize(canonical_arguments.size(), "canonical argument bytes do not fit UInt64");
    if (encoded_size > limits.maximum_canonical_argument_bytes)
        fail(ErrorCode::LimitExceeded, "canonical argument bytes exceed their specialization limit");
    const UInt64 formal_count = checkedSize(definition->getParameters().size(), "canonical argument count does not fit UInt64");
    /// specializeDefinition must later charge one request, one repeated pinned
    /// lookup, two full canonical-byte passes, and one unit per formal. Prove
    /// that unavoidable work fits before decode can allocate or enter a type
    /// factory.
    UInt64 guaranteed_work = 2;
    addProspectively(guaranteed_work, encoded_size, limits.maximum_work, "encoded specialization work");
    addProspectively(guaranteed_work, encoded_size, limits.maximum_work, "encoded specialization work");
    addProspectively(guaranteed_work, formal_count, limits.maximum_work, "encoded specialization work");
    ensureProspective(statistics.charged_work, guaranteed_work, limits.maximum_work, "encoded specialization work");
    try
    {
        auto decoded = CanonicalTypeArguments::decode(
            definition->getParameters(),
            canonical_arguments,
            limits.maximum_canonical_argument_bytes,
            limits.maximum_canonical_argument_item_bytes,
            type_argument_limits);
        return specialize(identity, decoded);
    }
    catch (const TemplateSpecializerError &)
    {
        throw;
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        if (error.code() == ErrorCodes::LIMIT_EXCEEDED)
            fail(ErrorCode::LimitExceeded, error.message());
        fail(ErrorCode::InvalidArguments, error.message());
    }
}

const ASTPtr & TemplateSpecializer::Attempt::State::getCanonicalPhysicalAST(TemplateSpecializationID id) const
{
    if (id >= entries.size() || entries[id]->state != MemoState::Complete || !entries[id]->ast)
        fail(ErrorCode::InvalidAttemptState, "specialization ID is absent or incomplete in this attempt");
    return entries[id]->ast;
}

TemplateSpecializationView TemplateSpecializer::Attempt::State::getSpecialization(TemplateSpecializationID id) const
{
    if (id >= entries.size() || entries[id]->state != MemoState::Complete || !entries[id]->ast)
        fail(ErrorCode::InvalidAttemptState, "specialization ID is absent or incomplete in this query memo");
    const auto & entry = *entries[id];
    if (entry.definition_handle_index >= definition_handles.size() || !definition_handles[entry.definition_handle_index])
        fail(ErrorCode::InvalidAttemptState, "specialization query memo lost its independent definition handle");
    return {
        .definition_identity = entry.identity,
        .canonical_arguments = entry.arguments,
        .canonical_physical_ast = entry.ast,
        .relative_occurrences = entry.occurrences,
        .definition_handle = definition_handles[entry.definition_handle_index],
    };
}

FinishedTemplateSpecializations TemplateSpecializer::Attempt::State::finish()
{
    if (!session)
        fail(ErrorCode::InvalidAttemptState, "specialization state has no pinned resolution session");
    for (const auto & entry : entries)
        if (!entry || entry->state != MemoState::Complete || !entry->ast)
            fail(ErrorCode::InvalidAttemptState, "specialization batch contains an incomplete entry");

    const UInt64 handle_count = checkedSize(definition_handles.size(), "definition handle count does not fit UInt64");
    chargeWork(3 * handle_count);
    std::vector<UInt32> handle_order(definition_handles.size());
    for (UInt32 index = 0; index < handle_order.size(); ++index)
        handle_order[index] = index;
    std::sort(
        handle_order.begin(),
        handle_order.end(),
        [&](UInt32 lhs, UInt32 rhs)
        {
            chargeWork();
            return identityLess(definition_handles[lhs]->getIdentity(), definition_handles[rhs]->getIdentity());
        });
    std::vector<UInt32> handle_remap(definition_handles.size());
    std::vector<Definition::Ptr> sorted_handles;
    sorted_handles.reserve(definition_handles.size());
    for (UInt32 sorted_index = 0; sorted_index < handle_order.size(); ++sorted_index)
    {
        handle_remap[handle_order[sorted_index]] = sorted_index;
        sorted_handles.push_back(std::move(definition_handles[handle_order[sorted_index]]));
    }

    FinishedTemplateSpecializations result;
    chargeWork(checkedSize(entries.size(), "specialization result count does not fit UInt64"));
    result.specializations.reserve(entries.size());
    /// Retained-path construction was charged while the paths were built.
    /// This separate prospective charge accounts for the final normalization
    /// pass and must succeed before any path is mutated.
    chargeWork(statistics.retained_path_components);
    for (auto & entry : entries)
    {
        for (auto & occurrence : entry->occurrences)
            std::reverse(occurrence.path.begin(), occurrence.path.end());
        result.specializations.push_back({
            .definition_identity = entry->identity,
            .definition_handle_index = handle_remap[entry->definition_handle_index],
            .canonical_arguments = std::move(entry->arguments),
            .canonical_physical_ast = std::move(entry->ast),
            .relative_occurrences = std::move(entry->occurrences),
        });
    }
    result.definition_handles = std::move(sorted_handles);
    result.statistics = statistics;
    session.reset();
    return result;
}

TemplateSpecializer::Attempt::Attempt(std::unique_ptr<State> state_)
    : state(std::move(state_))
{
}

TemplateSpecializer::Attempt::Attempt(Attempt && other) noexcept
    : state(std::move(other.state))
    , finished(other.finished)
    , poisoned(other.poisoned)
{
    other.finished = true;
    other.poisoned = true;
}

TemplateSpecializer::Attempt::~Attempt() = default;

TemplateSpecializer::Attempt TemplateSpecializer::Attempt::begin(
    const IAuthorityAdapter & authority, const TemplateSpecializerLimits & limits, ProspectiveResourceBudget * query_budget)
{
    return beginImpl(authority, limits, query_budget, false);
}

TemplateSpecializer::Attempt TemplateSpecializer::Attempt::beginImpl(
    const IAuthorityAdapter & authority,
    const TemplateSpecializerLimits & limits,
    ProspectiveResourceBudget * query_budget,
    bool retain_query_memo)
{
    validateTemplateSpecializerLimits(limits);
    const TypeAuthorityCapabilities capabilities = authority.getCapabilities();
    constexpr TypeAuthorityCapabilityMask required = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    if (capabilities.adapter_abi != 1 || !capabilities.containsAll(required))
        fail(ErrorCode::MissingCapability, "authority does not advertise transient template specialization");
    const auto & authority_limits = capabilities.limits;
    if (authority_limits.maximum_definitions == 0 || authority_limits.maximum_definition_bytes == 0
        || authority_limits.maximum_template_nodes == 0 || authority_limits.maximum_direct_dependencies == 0
        || authority_limits.maximum_transitive_dependencies == 0 || authority_limits.maximum_checker_work == 0)
        fail(ErrorCode::MissingCapability, "authority advertises an invalid Limits tuple");
    const UUID database_uuid = authority.getDatabaseUUID();
    if (database_uuid == UUIDHelpers::Nil)
        fail(ErrorCode::AuthorityFailure, "authority exposes a nil database UUID");
    if (retain_query_memo)
    {
        if (!query_budget)
            fail(ErrorCode::InvalidAttemptState, "query memo retention requires a query resource budget");
        State::chargeInitialQueryMemoRetention(limits, *query_budget);
    }
    try
    {
        authority.requireCapabilities(required, "user-defined type template specialization");
        auto session = authority.beginResolutionSession();
        return Attempt(std::make_unique<State>(database_uuid, capabilities, limits, std::move(session), query_budget, retain_query_memo));
    }
    catch (const TemplateSpecializerError &)
    {
        throw;
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(ErrorCode::AuthorityFailure, fmt::format("authority could not begin a resolution session: {}", error.message()));
    }
}

[[noreturn]] void TemplateSpecializer::Attempt::invalidState(std::string_view operation) const
{
    fail(
        ErrorCode::InvalidAttemptState,
        fmt::format(
            "cannot {} a {} specialization attempt",
            operation,
            poisoned       ? "poisoned"
                : finished ? "finished/moved-from"
                           : "empty"));
}

void TemplateSpecializer::Attempt::poison() noexcept
{
    state.reset();
    poisoned = true;
}

TemplateSpecializationID
TemplateSpecializer::Attempt::specialize(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
{
    if (!state || finished || poisoned)
        invalidState("specialize through");
    try
    {
        return state->specialize(identity, arguments);
    }
    catch (...)
    {
        poison();
        throw;
    }
}

TemplateSpecializationID TemplateSpecializer::Attempt::specializeEncoded(
    const DefinitionIdentity & identity, std::string_view canonical_arguments, const CanonicalTypeArgumentLimits & type_argument_limits)
{
    if (!state || finished || poisoned)
        invalidState("specialize encoded arguments through");
    try
    {
        return state->specializeEncoded(identity, canonical_arguments, type_argument_limits);
    }
    catch (...)
    {
        poison();
        throw;
    }
}

const ASTPtr & TemplateSpecializer::Attempt::getCanonicalPhysicalAST(TemplateSpecializationID id)
{
    if (!state || finished || poisoned)
        invalidState("borrow from");
    try
    {
        return state->getCanonicalPhysicalAST(id);
    }
    catch (...)
    {
        poison();
        throw;
    }
}

void TemplateSpecializer::Attempt::closeQueryMemo() noexcept
{
    state.reset();
    finished = true;
}

void TemplateSpecializer::Attempt::releaseQueryMemoSession() noexcept
{
    if (state && !finished && !poisoned)
        state->releaseResolutionSession();
}

TemplateSpecializationID
TemplateSpecializer::Attempt::specializeFromQueryMemo(
    const IAuthorityAdapter & authority, const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
{
    if (!state || finished || poisoned)
        invalidState("specialize through query memo");
    try
    {
        if (authority.getDatabaseUUID() != state->getAuthorityDatabaseUUID())
            fail(ErrorCode::InvalidIdentity, "query specialization memo belongs to another authority database");
        state->attachResolutionSession(authority.beginResolutionSession());
        const auto result = state->specializeFromQueryMemo(identity, arguments);
        state->releaseResolutionSession();
        return result;
    }
    catch (...)
    {
        poison();
        throw;
    }
}

TemplateSpecializationView TemplateSpecializer::Attempt::getSpecialization(TemplateSpecializationID id) const
{
    if (!state || finished || poisoned)
        invalidState("borrow a completed specialization from");
    return state->getSpecialization(id);
}

const TemplateSpecializerStatistics & TemplateSpecializer::Attempt::getStatistics() const
{
    if (!state || finished || poisoned)
        invalidState("inspect statistics on");
    return state->getStatistics();
}

const TemplateSpecializerLimits & TemplateSpecializer::Attempt::getLimits() const
{
    if (!state || finished || poisoned)
        invalidState("inspect limits on");
    return state->getLimits();
}

UUID TemplateSpecializer::Attempt::getAuthorityDatabaseUUID() const
{
    if (!state || finished || poisoned)
        invalidState("inspect authority identity on");
    return state->getAuthorityDatabaseUUID();
}

UInt64 TemplateSpecializer::Attempt::getAuthorityGeneration() const
{
    if (!state || finished || poisoned)
        invalidState("inspect authority generation on");
    return state->getAuthorityGeneration();
}

FinishedTemplateSpecializations TemplateSpecializer::Attempt::finish()
{
    if (!state || finished || poisoned)
        invalidState("finish");
    try
    {
        auto result = state->finish();
        state.reset();
        finished = true;
        return result;
    }
    catch (...)
    {
        poison();
        throw;
    }
}

TemplateSpecializer::QueryMemo::QueryMemo(
    const IAuthorityAdapter & authority, const TemplateSpecializerLimits & limits, ProspectiveResourceBudget & query_budget)
    : resource_budget(std::addressof(query_budget))
    , attempt(Attempt::beginImpl(authority, limits, resource_budget, true))
{
    /// beginImpl establishes the exact generation once; no root lease may
    /// survive construction or any individual specialize() call.
    attempt.releaseQueryMemoSession();
}

TemplateSpecializer::QueryMemo::~QueryMemo()
{
    attempt.closeQueryMemo();
}

TemplateSpecializationID
TemplateSpecializer::QueryMemo::specialize(
    const IAuthorityAdapter & authority, const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
{
    return attempt.specializeFromQueryMemo(authority, identity, arguments);
}

const ASTPtr & TemplateSpecializer::QueryMemo::getCanonicalPhysicalAST(TemplateSpecializationID id)
{
    return attempt.getCanonicalPhysicalAST(id);
}

TemplateSpecializationView TemplateSpecializer::QueryMemo::getSpecialization(TemplateSpecializationID id) const
{
    return attempt.getSpecialization(id);
}

const TemplateSpecializerStatistics & TemplateSpecializer::QueryMemo::getStatistics() const
{
    return attempt.getStatistics();
}

const TemplateSpecializerLimits & TemplateSpecializer::QueryMemo::getLimits() const
{
    return attempt.getLimits();
}

UUID TemplateSpecializer::QueryMemo::getAuthorityDatabaseUUID() const
{
    return attempt.getAuthorityDatabaseUUID();
}

UInt64 TemplateSpecializer::QueryMemo::getAuthorityGeneration() const
{
    return attempt.getAuthorityGeneration();
}

bool TemplateSpecializer::QueryMemo::usesResourceBudget(const ProspectiveResourceBudget & query_budget_) const noexcept
{
    return resource_budget == std::addressof(query_budget_);
}

}
