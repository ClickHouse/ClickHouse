#pragma once

#include <Core/ProtocolDefines.h>
#include <IO/LimitReadBuffer.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/SetSerialization.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <fmt/format.h>

#include <concepts>
#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>

/// A step manifest declares, once, what a query plan step puts on the wire in the framed format
/// (plan version `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE` and above). The step
/// declares a plain aggregate, its wire struct, whose members are exactly the values that travel,
/// and a `constexpr` manifest that names each member, gives it a digest class and places it in a
/// payload format. The framework then writes and reads the payload, fills the settings channel,
/// derives the registry entry and describes the declaration for the baseline test. The step keeps
/// two functions, `toWire` and `fromWire`.
///
///     struct LimitWire
///     {
///         UInt64 limit = 0;
///         UInt64 offset = 0;
///         bool with_ties = false;
///         SortDescription description;
///     };
///
///     constexpr auto LIMIT_MANIFEST = StepManifest<LimitStep, LimitWire>("Limit")
///         .nameIntroducedIn(1)
///         .baseFormat(
///             field("limit", WireFieldClass::Logical, &LimitWire::limit),
///             field("offset", WireFieldClass::Logical, &LimitWire::offset),
///             field("with_ties", WireFieldClass::Logical, &LimitWire::with_ties),
///             field("description", WireFieldClass::Logical, &LimitWire::description));
///
/// Every payload member is always encoded, in declaration order of the manifest. A member that
/// is present or absent is an `std::optional`. A wire struct member that the manifest does not
/// bind, or binds twice, fails to compile where the manifest is registered. Streams below the
/// framed base version keep the step's hand-written serializer.

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/// A codec: a member type that has its own serialization, shared by several steps or owned by one.
/// A specialization gives the type a name, a writer and a reader; the framework then treats the type
/// like any other member. The shared codecs are specialized below; a step specializes the trait for
/// its own types in its own source file, before its manifest.
template <typename T>
struct WireCodec;

template <typename T>
concept HasWireCodec = requires (const T & value, T & target, IQueryPlanStep::Serialization & out, IQueryPlanStep::Deserialization & in)
{
    { WireCodec<T>::name } -> std::convertible_to<const char *>;
    WireCodec<T>::write(value, out);
    WireCodec<T>::read(target, in);
};

template <>
struct WireCodec<SortDescription>
{
    static constexpr const char * name = "SortDescription";
    static void write(const SortDescription & value, IQueryPlanStep::Serialization & ctx) { serializeSortDescription(value, ctx.out); }
    static void read(SortDescription & value, IQueryPlanStep::Deserialization & ctx) { deserializeSortDescription(value, ctx.in); }
};

template <>
struct WireCodec<ActionsDAG>
{
    static constexpr const char * name = "ActionsDAG";
    static void write(const ActionsDAG & value, IQueryPlanStep::Serialization & ctx) { value.serialize(ctx.out, ctx.registry); }
    static void read(ActionsDAG & value, IQueryPlanStep::Deserialization & ctx)
    {
        value = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity, bytesRemainingInFrame(ctx.in));
    }
};

template <>
struct WireCodec<AggregateDescriptions>
{
    static constexpr const char * name = "AggregateDescriptions";
    static void write(const AggregateDescriptions & value, IQueryPlanStep::Serialization & ctx) { serializeAggregateDescriptions(value, ctx.out); }
    static void read(AggregateDescriptions & value, IQueryPlanStep::Deserialization & ctx)
    {
        deserializeAggregateDescriptions(value, ctx.in, ctx.max_type_complexity);
    }
};

template <>
struct WireCodec<TableExpressionModifiers::Rational>
{
    static constexpr const char * name = "Rational";
    static void write(const TableExpressionModifiers::Rational & value, IQueryPlanStep::Serialization & ctx) { serializeRational(value, ctx.out); }
    static void read(TableExpressionModifiers::Rational & value, IQueryPlanStep::Deserialization & ctx) { value = deserializeRational(ctx.in); }
};

/// Which digest an entry belongs to, the classification of PR 116196: a `Logical` entry decides
/// which rows the step computes, a `Physical` entry only how. Both are in the full digest; only
/// `Logical` entries are in the logical digest.
enum class WireFieldClass : UInt8
{
    Logical,
    Physical,
};

/// One payload member: its name, its digest class and the wire struct member it lives in.
/// `in_cache_key` says whether the field takes part in the cache key (the runtime-statistics hash;
/// see `writeManifestPayload`). A `Logical` field takes part by default; mark it `notInCacheKey` when
/// it changes the rows but not the statistics the key identifies (a partial-vs-final aggregation, say).
/// A `Physical` field never takes part, whatever this says.
template <typename Wire_, typename T>
struct WireField
{
    using Wire = Wire_;
    using Value = T;

    const char * name = nullptr;
    WireFieldClass field_class = WireFieldClass::Logical;
    T Wire::* member = nullptr;
    bool in_cache_key = true;

    constexpr WireField notInCacheKey() const
    {
        auto copy = *this;
        copy.in_cache_key = false;
        return copy;
    }
};

template <typename Wire, typename T>
constexpr WireField<Wire, T> field(const char * name, WireFieldClass field_class, T Wire::* member)
{
    return {name, field_class, member};
}

/// One value that travels through the settings channel under a plan setting name. The plan version
/// that added the setting and the receiver's default live in `QueryPlanSerializationSettings`; the
/// entry is written only when the value differs from that default.
template <typename Wire_, typename T, typename SettingField>
struct WireSetting
{
    using Wire = Wire_;
    using Value = T;
    using Index = SettingIndex<QueryPlanSerializationSettings, SettingField>;

    const Index * setting;
    WireFieldClass field_class;
    T Wire::* member;
};

template <typename Wire, typename T, typename SettingField>
constexpr WireSetting<Wire, T, SettingField> setting(
    const SettingIndex<QueryPlanSerializationSettings, SettingField> & index, WireFieldClass field_class, T Wire::* member)
{
    return {&index, field_class, member};
}

/// Digest eligibility predicates over the wire struct, the per-instance eligibility of PR 116196.
struct Eligible
{
    template <typename Wire>
    static constexpr bool always(const Wire &) { return true; }

    template <typename Wire>
    static constexpr bool never(const Wire &) { return false; }
};

template <typename Step_, typename Wire_, typename Fields = std::tuple<>, typename Settings = std::tuple<>>
struct StepManifest
{
    using Step = Step_;
    using Wire = Wire_;
    using Eligibility = bool (*)(const Wire &);

    const char * name;
    UInt64 name_introduced_in = 0;
    /// Whether a payload format was declared. A step that travels only through the settings channel
    /// declares none, and then carries an empty payload.
    bool has_base_format = false;
    /// The payload members, in declaration order. Every one is always encoded.
    Fields fields{};
    Settings setting_entries{};
    Eligibility full_digest_eligible = &Eligible::always<Wire>;
    Eligibility logical_digest_eligible = &Eligible::always<Wire>;
    /// The number of input streams the step reads: -1 derive from the step's base class (a source
    /// has none, a transforming step has one), -2 a variable number, or a fixed count.
    int input_arity = -1;

    static constexpr size_t variable_input_count = std::numeric_limits<size_t>::max();

    constexpr explicit StepManifest(const char * name_) : name(name_) { }

    constexpr StepManifest(
        const char * name_,
        UInt64 name_introduced_in_,
        bool has_base_format_,
        Fields fields_,
        Settings setting_entries_,
        Eligibility full_digest_eligible_,
        Eligibility logical_digest_eligible_,
        int input_arity_)
        : name(name_)
        , name_introduced_in(name_introduced_in_)
        , has_base_format(has_base_format_)
        , fields(fields_)
        , setting_entries(setting_entries_)
        , full_digest_eligible(full_digest_eligible_)
        , logical_digest_eligible(logical_digest_eligible_)
        , input_arity(input_arity_)
    {
    }

    /// The plan version that added this serialization name.
    constexpr StepManifest nameIntroducedIn(UInt64 version) const
    {
        StepManifest copy = *this;
        copy.name_introduced_in = version;
        return copy;
    }

    /// The payload members. Its plan version is the framed base version, or the name's version when
    /// that is higher.
    template <typename... F>
    constexpr auto baseFormat(F... fields_) const
    {
        static_assert(std::tuple_size_v<Fields> == 0, "the payload is declared once");
        static_assert((std::is_same_v<typename F::Wire, Wire> && ...), "every field must belong to the manifest's wire struct");
        using NewFields = std::tuple<F...>;
        return StepManifest<Step, Wire, NewFields, Settings>(
            name, name_introduced_in, true, NewFields{fields_...},
            setting_entries, full_digest_eligible, logical_digest_eligible, input_arity);
    }

    /// The values the step sends through the settings channel.
    template <typename... S>
    constexpr auto settings(S... entries) const
    {
        static_assert(std::tuple_size_v<Settings> == 0, "the settings are declared once");
        static_assert((std::is_same_v<typename S::Wire, Wire> && ...), "every setting must belong to the manifest's wire struct");
        using NewSettings = std::tuple<S...>;
        return StepManifest<Step, Wire, Fields, NewSettings>(
            name, name_introduced_in, has_base_format, fields, NewSettings{entries...},
            full_digest_eligible, logical_digest_eligible, input_arity);
    }

    constexpr StepManifest fullDigest(Eligibility eligible) const
    {
        StepManifest copy = *this;
        copy.full_digest_eligible = eligible;
        return copy;
    }

    constexpr StepManifest logicalDigest(Eligibility eligible) const
    {
        StepManifest copy = *this;
        copy.logical_digest_eligible = eligible;
        return copy;
    }

    /// The step reads exactly `count` input streams. Needed only for a step that derives from
    /// neither a source nor a transforming step; the others derive it from their base class.
    constexpr StepManifest inputs(size_t count) const
    {
        StepManifest copy = *this;
        copy.input_arity = static_cast<int>(count);
        return copy;
    }

    /// The step reads a variable number of input streams, e.g. a union.
    constexpr StepManifest variableInputs() const
    {
        StepManifest copy = *this;
        copy.input_arity = -2;
        return copy;
    }

    /// The number of input streams the step reads, or `variable_input_count` when it varies.
    constexpr size_t inputCount() const
    {
        if (input_arity == -2)
            return variable_input_count;
        if (input_arity >= 0)
            return static_cast<size_t>(input_arity);
        if constexpr (std::is_base_of_v<ISourceStep, Step>)
            return 0;
        else if constexpr (std::is_base_of_v<ITransformingStep, Step>)
            return 1;
        else
            return variable_input_count;
    }

    /// Whether the arity is known: it is declared, or the base class fixes it. A step that is
    /// neither a source nor a transforming step must declare it.
    constexpr bool arityIsResolved() const
    {
        return input_arity != -1 || std::is_base_of_v<ISourceStep, Step> || std::is_base_of_v<ITransformingStep, Step>;
    }

    /// The plan version of the payload: the framed base version, or the name's version when higher.
    constexpr UInt64 formatVersion() const
    {
        return std::max<UInt64>(DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE, name_introduced_in);
    }
};


namespace WireDetail
{

/// Converts to anything, so `Wire{AnyValue{}, AnyValue{}}` is valid exactly when the aggregate has at
/// least two members. Never defined: it is only used inside unevaluated requirements.
struct AnyValue
{
    template <typename T>
    operator T() const; /// NOLINT
};

template <typename T, size_t... I>
constexpr bool constructibleWith(std::index_sequence<I...>)
{
    return requires { T{((void)I, AnyValue{})...}; };
}

/// The number of members of an aggregate: the largest count of values it can be brace-initialized
/// with. A member that is itself an aggregate takes one value, because `AnyValue` converts to it
/// directly and no brace elision happens.
template <typename T, size_t N = 0>
constexpr size_t aggregateMemberCount()
{
    static_assert(std::is_aggregate_v<T>, "a wire struct is a plain aggregate: public members, no constructors, no base class");
    static_assert(N <= 64, "a wire struct with more than 64 members is not supported");
    if constexpr (constructibleWith<T>(std::make_index_sequence<N + 1>{}))
        return aggregateMemberCount<T, N + 1>();
    else
        return N;
}

template <typename A, typename B>
constexpr bool sameMember(A a, B b)
{
    if constexpr (std::is_same_v<A, B>)
        return a == b;
    else
        return false;
}

template <size_t I, size_t J, typename Tuple>
constexpr bool distinctFromLater(const Tuple & bindings)
{
    if constexpr (J >= std::tuple_size_v<Tuple>)
        return true;
    else
        return !sameMember(std::get<I>(bindings).member, std::get<J>(bindings).member) && distinctFromLater<I, J + 1>(bindings);
}

template <size_t I, typename Tuple>
constexpr bool allDistinct(const Tuple & bindings)
{
    if constexpr (I >= std::tuple_size_v<Tuple>)
        return true;
    else
        return distinctFromLater<I, I + 1>(bindings) && allDistinct<I + 1>(bindings);
}

template <typename Tuple, typename F>
constexpr void forEach(const Tuple & tuple, F && f)
{
    std::apply([&](const auto &... element) { (f(element), ...); }, tuple);
}

/// Every binding of a manifest, the payload fields and then the settings, as one tuple.
template <typename Manifest>
constexpr auto allBindings(const Manifest & manifest)
{
    return std::tuple_cat(manifest.fields, manifest.setting_entries);
}

template <typename T>
inline constexpr bool is_optional = false;
template <typename T>
inline constexpr bool is_optional<std::optional<T>> = true;

template <typename T>
inline constexpr bool is_vector = false;
template <typename T, typename A>
inline constexpr bool is_vector<std::vector<T, A>> = true;

template <typename T>
inline constexpr bool is_pair = false;
template <typename A, typename B>
inline constexpr bool is_pair<std::pair<A, B>> = true;

[[noreturn]] void throwCannotParse(const char * what);

/// A length or count that is about to be allocated must fit into the bytes that remain in the frame.
/// A framed payload is read inside a `LimitReadBuffer`, so what remains is exactly the payload's tail.
inline void checkFitsRemaining(UInt64 count, ReadBuffer & in, const char * what)
{
    if (count > bytesRemainingInFrame(in))
        throwCannotParse(what);
}

}


/// One fixed encoding per wire member type. A different encoding is a different type, never a change.
namespace WireEncoding
{

template <typename T>
void write(const T & value, IQueryPlanStep::Serialization & ctx)
{
    auto & out = ctx.out;
    if constexpr (HasWireCodec<T>)
        WireCodec<T>::write(value, ctx);
    else if constexpr (std::is_same_v<T, bool>)
        writeBinary(UInt8(value ? 1 : 0), out);
    else if constexpr (std::is_unsigned_v<T> && std::is_integral_v<T>)
        writeVarUInt(UInt64(value), out);
    else if constexpr (std::is_enum_v<T>)
    {
        /// Enums travel as their underlying value; a value list is not declared until a payload
        /// member needs one, so today they occur only as settings-channel members.
        const auto underlying = static_cast<std::underlying_type_t<T>>(value);
        if (underlying < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A negative enum value has no wire encoding");
        writeVarUInt(UInt64(underlying), out);
    }
    else if constexpr (std::is_same_v<T, Float64> || std::is_same_v<T, Float32>)
        writeBinaryLittleEndian(value, out);
    else if constexpr (std::is_same_v<T, String>)
        writeStringBinary(value, out);
    else if constexpr (WireDetail::is_optional<T>)
    {
        writeBinary(UInt8(value.has_value() ? 1 : 0), out);
        if (value.has_value())
            write(*value, ctx);
    }
    else if constexpr (WireDetail::is_pair<T>)
    {
        write(value.first, ctx);
        write(value.second, ctx);
    }
    else if constexpr (WireDetail::is_vector<T>)
    {
        writeVarUInt(value.size(), out);
        for (const auto & element : value)
            write(element, ctx);
    }
    else
        static_assert(sizeof(T) == 0, "this type has no wire encoding; use a supported type or add a codec type");
}

template <typename T>
void read(T & value, IQueryPlanStep::Deserialization & ctx)
{
    auto & in = ctx.in;
    if constexpr (HasWireCodec<T>)
        WireCodec<T>::read(value, ctx);
    else if constexpr (std::is_same_v<T, bool>)
    {
        UInt8 byte = 0;
        readBinary(byte, in);
        if (byte > 1)
            WireDetail::throwCannotParse("a bool must be 0 or 1");
        value = byte == 1;
    }
    else if constexpr (std::is_unsigned_v<T> && std::is_integral_v<T>)
    {
        UInt64 wide = 0;
        readVarUInt(wide, in);
        if (wide > std::numeric_limits<T>::max())
            WireDetail::throwCannotParse("an integer is out of range for its member");
        value = static_cast<T>(wide);
    }
    else if constexpr (std::is_enum_v<T>)
    {
        UInt64 wide = 0;
        readVarUInt(wide, in);
        if (wide > static_cast<UInt64>(std::numeric_limits<std::underlying_type_t<T>>::max()))
            WireDetail::throwCannotParse("an enum value is out of range for its member");
        value = static_cast<T>(wide);
    }
    else if constexpr (std::is_same_v<T, Float64> || std::is_same_v<T, Float32>)
        readBinaryLittleEndian(value, in);
    else if constexpr (std::is_same_v<T, String>)
    {
        UInt64 length = 0;
        readVarUInt(length, in);
        WireDetail::checkFitsRemaining(length, in, "a string is longer than the payload");
        value.resize(length);
        in.readStrict(value.data(), length);
    }
    else if constexpr (WireDetail::is_optional<T>)
    {
        UInt8 present = 0;
        readBinary(present, in);
        if (present > 1)
            WireDetail::throwCannotParse("a presence byte must be 0 or 1");
        if (present)
        {
            value.emplace();
            read(*value, ctx);
        }
        else
            value.reset();
    }
    else if constexpr (WireDetail::is_pair<T>)
    {
        read(value.first, ctx);
        read(value.second, ctx);
    }
    else if constexpr (WireDetail::is_vector<T>)
    {
        UInt64 count = 0;
        readVarUInt(count, in);
        WireDetail::checkFitsRemaining(count, in, "a count is larger than the payload");
        value.resize(count);
        for (auto & element : value)
            read(element, ctx);
    }
    else
        static_assert(sizeof(T) == 0, "this type has no wire encoding; use a supported type or add a codec type");
}

/// The canonical name of a member type, for the baseline.
template <typename T>
String typeName()
{
    if constexpr (HasWireCodec<T>)
        return WireCodec<T>::name;
    else if constexpr (std::is_same_v<T, bool>)
        return "bool";
    else if constexpr (std::is_unsigned_v<T> && std::is_integral_v<T>)
        return "UInt" + std::to_string(sizeof(T) * 8);
    else if constexpr (std::is_enum_v<T>)
        return "enum" + std::to_string(sizeof(std::underlying_type_t<T>) * 8);
    else if constexpr (std::is_same_v<T, Float64>)
        return "Float64";
    else if constexpr (std::is_same_v<T, Float32>)
        return "Float32";
    else if constexpr (std::is_same_v<T, String>)
        return "String";
    else if constexpr (WireDetail::is_optional<T>)
        return "optional<" + typeName<typename T::value_type>() + ">";
    else if constexpr (WireDetail::is_pair<T>)
        return "pair<" + typeName<typename T::first_type>() + "," + typeName<typename T::second_type>() + ">";
    else if constexpr (WireDetail::is_vector<T>)
        return "vector<" + typeName<typename T::value_type>() + ">";
    else
        static_assert(sizeof(T) == 0, "this type has no wire encoding");
}

}


/// The generated operations over a manifest.

/// Writes the payload: every member, in declaration order. Each step name owns one payload layout, so
/// the format version is always 1; a step that changes its wire content takes a new name instead.
template <typename Manifest>
void writeManifestPayload(const Manifest & manifest, const typename Manifest::Wire & wire, IQueryPlanStep::Serialization & ctx)
{
    WireDetail::forEach(manifest.fields, [&](const auto & field)
    {
        /// The cache key identifies a step for runtime statistics, so it hashes only the fields that
        /// affect those statistics: the logical fields that are not marked out, and never a physical
        /// one. A step that differs from another only in what is skipped here hashes the same, which is
        /// what lets the single-node and parallel-replicas builds of a query share a statistics entry.
        /// These bytes are only ever hash input, never read back, so skipping is safe; the real
        /// transport (`for_cache_key` false) writes every field.
        if (ctx.for_cache_key && (field.field_class == WireFieldClass::Physical || !field.in_cache_key))
            return;
        WireEncoding::write(wire.*field.member, ctx);
    });
    ctx.step_format_version = 1;
}

/// Fills the setting members of the wire struct from a settings object.
template <typename Manifest>
void readManifestSettings(const Manifest & manifest, typename Manifest::Wire & wire, const QueryPlanSerializationSettings & settings)
{
    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        wire.*entry.member = static_cast<Value>(settings[*entry.setting]);
    });
}

/// Reads the payload into a default-constructed wire struct: the settings first, from the node's
/// settings entries, then every payload member. Each step name owns one payload layout, so the reader
/// consumes the whole frame; the framed reader refuses a payload that leaves bytes behind.
template <typename Manifest>
typename Manifest::Wire readManifestPayload(const Manifest & manifest, IQueryPlanStep::Deserialization & ctx)
{
    using Wire = typename Manifest::Wire;
    Wire wire{};
    readManifestSettings(manifest, wire, ctx.settings);
    WireDetail::forEach(manifest.fields, [&](const auto & field) { WireEncoding::read(wire.*field.member, ctx); });
    return wire;
}

/// Fills the settings channel: a setting is written only when its value differs from the registered
/// default, which keeps a setting a receiver does not know off the wire whenever it sits at its
/// default. This relies on the registered defaults being frozen wire defaults: an absent setting is
/// reconstructed from the receiver's registered default, so changing a registered default would make
/// an old writer and a new reader disagree. A new setting gets a new name rather than a changed
/// default, and `registerManifest` checks that a wire struct's initializer equals the registered
/// default. The frame raises the reader requirement for a setting a target does not know.
template <typename Manifest>
void writeManifestSettings(const Manifest & manifest, const typename Manifest::Wire & wire, QueryPlanSerializationSettings & settings)
{
    static const QueryPlanSerializationSettings defaults;
    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        const Value & value = wire.*entry.member;
        if (value != static_cast<Value>(defaults[*entry.setting]))
            settings[*entry.setting] = value;
    });
}

/// Calls `visitor(name, field_class, value)` for every payload field and every setting, in
/// declaration order. This is the input of the digests: the full digest takes every entry, the
/// logical digest the `Logical` ones.
template <typename Manifest, typename Visitor>
void forEachWireEntry(const Manifest & manifest, const typename Manifest::Wire & wire, Visitor && visitor)
{
    WireDetail::forEach(manifest.fields, [&](const auto & field) { visitor(field.name, field.field_class, wire.*field.member); });
    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        visitor(QueryPlanSerializationSettings::settingName(*entry.setting), entry.field_class, wire.*entry.member);
    });
}

/// The registry entry a manifest describes: the name's plan version and the input count. Every
/// framed step is one payload format, so the maximum format version is 1.
template <typename Manifest>
QueryPlanStepRegistry::StepSerializationInfo manifestRegistryInfo(const Manifest & manifest)
{
    QueryPlanStepRegistry::StepSerializationInfo info;
    info.introduced_in_plan_version = manifest.name_introduced_in;
    info.has_wire_struct = true;
    info.max_format_version = 1;
    info.input_count = manifest.inputCount();
    return info;
}

/// The manifest in a canonical text form, one line per node, for the baseline test.
template <typename Manifest>
String describeManifest(const Manifest & manifest)
{
    using Wire = typename Manifest::Wire;
    WriteBufferFromOwnString out;
    out << "name " << manifest.name << " introduced_in " << manifest.name_introduced_in
        << " full_digest " << (manifest.full_digest_eligible == &Eligible::always<Wire> ? "always" : manifest.full_digest_eligible == &Eligible::never<Wire> ? "never" : "predicate")
        << " logical_digest " << (manifest.logical_digest_eligible == &Eligible::always<Wire> ? "always" : manifest.logical_digest_eligible == &Eligible::never<Wire> ? "never" : "predicate")
        << "\n";

    if (manifest.has_base_format)
    {
        out << "format 1 introduced_in " << manifest.formatVersion() << "\n";
        WireDetail::forEach(manifest.fields, [&](const auto & field)
        {
            using Value = typename std::remove_cvref_t<decltype(field)>::Value;
            out << "  field " << field.name << " " << (field.field_class == WireFieldClass::Logical ? "Logical" : "Physical")
                << (field.in_cache_key ? "" : " no-cache-key")
                << " " << WireEncoding::typeName<Value>() << "\n";
        });
    }

    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        out << "  setting " << QueryPlanSerializationSettings::settingName(*entry.setting) << " "
            << (entry.field_class == WireFieldClass::Logical ? "Logical" : "Physical") << " " << WireEncoding::typeName<Value>() << "\n";
    });

    if (manifest.has_base_format)
    {
        /// The initializers, as the payload of a default-constructed wire struct: what a reader
        /// reconstructs for every value that is not on the wire.
        WriteBufferFromOwnString payload;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization ctx{payload, registry};
        ctx.version = std::numeric_limits<UInt64>::max();
        writeManifestPayload(manifest, Wire{}, ctx);
        payload.finalize();
        out << "  initializers ";
        for (unsigned char c : payload.str())
            out << fmt::format("{:02x}", c);
        out << "\n";
    }

    out.finalize();
    return out.str();
}

/// The coverage rule: the manifest binds every member of the wire struct exactly once.
template <typename Manifest>
constexpr bool manifestCoversWire(const Manifest & manifest)
{
    using Wire = typename Manifest::Wire;
    constexpr size_t members = WireDetail::aggregateMemberCount<Wire>();
    const auto bindings = WireDetail::allBindings(manifest);
    if (std::tuple_size_v<decltype(bindings)> != members)
        return false;
    return WireDetail::allDistinct<0>(bindings);
}

/// The initializer of a setting member is what a default-constructed wire struct means, and what a
/// reader gets for an absent entry is the registered default: the two must agree, or the baseline
/// would pin one meaning and the reader apply another.
template <typename Manifest>
void checkSettingInitializers(const Manifest & manifest)
{
    using Wire = typename Manifest::Wire;
    static const QueryPlanSerializationSettings defaults;
    const Wire initializers{};
    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        if (initializers.*entry.member != static_cast<Value>(defaults[*entry.setting]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The wire struct of step '{}' initializes the member of setting '{}' to a value other than the setting's default",
                manifest.name, QueryPlanSerializationSettings::settingName(*entry.setting));
    });
}

/// Registers a step by its manifest: the registry entry is derived and the description of the
/// declaration goes with it. Fails to compile unless the manifest binds every member of the wire
/// struct exactly once.
template <const auto & manifest>
void registerManifest(QueryPlanStepRegistry & registry, QueryPlanStepRegistry::StepCreateFunction create)
{
    static_assert(manifestCoversWire(manifest), "the manifest must bind every member of its wire struct exactly once");
    static_assert(manifest.arityIsResolved(),
        "declare the step's input count with .inputs(n) or .variableInputs(): it derives from neither a source nor a transforming step");
    checkSettingInitializers(manifest);
    registry.registerStep(manifest.name, std::move(create), manifestRegistryInfo(manifest), describeManifest(manifest));
}

/// Whether a stream is framed, and the generated path applies, or older, and the hand-written one does.
inline bool usesManifest(UInt64 stream_version)
{
    return stream_version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
}

}
