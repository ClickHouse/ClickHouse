#pragma once

#include <Core/ProtocolDefines.h>
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
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <fmt/format.h>

#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>

/// A step manifest declares, once, what a query plan step puts on the wire in the framed format
/// (plan version `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE` and above). The step
/// declares a plain aggregate, its wire struct, whose members are exactly the values that travel,
/// and a `constexpr` manifest that names each member, gives it a digest class and places it in a
/// payload format. The framework then writes and reads the payload, fills the settings channel,
/// raises the reader requirement for appended values, derives the registry entry and describes the
/// declaration for the baseline test. The step keeps two functions, `toWire` and `fromWire`.
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
    extern const int LOGICAL_ERROR;
}

/// Which digest an entry belongs to, the classification of PR 116196: a `Logical` entry decides
/// which rows the step computes, a `Physical` entry only how. Both are in the full digest; only
/// `Logical` entries are in the logical digest.
enum class WireFieldClass : UInt8
{
    Logical,
    Physical,
};

/// The plan version that added an appended payload format.
struct IntroducedIn
{
    UInt64 version;
};

/// One payload member: its name, its digest class and the wire struct member it lives in.
template <typename Wire_, typename T>
struct WireField
{
    using Wire = Wire_;
    using Value = T;

    const char * name;
    WireFieldClass field_class;
    T Wire::* member;
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

/// The fields of one payload format. `introduced_in` is 0 for the base format, whose plan version
/// is derived from the framed base version and the name's version.
template <typename... Fields>
struct WireFormat
{
    UInt64 introduced_in;
    std::tuple<Fields...> fields;
};

/// The wire struct of a step that keeps a hand-written serializer: nothing is declared.
struct NoWire
{
};

/// Digest eligibility predicates over the wire struct, the per-instance eligibility of PR 116196.
struct Eligible
{
    template <typename Wire>
    static constexpr bool always(const Wire &) { return true; }

    template <typename Wire>
    static constexpr bool never(const Wire &) { return false; }
};

template <typename Step_, typename Wire_, typename Formats = std::tuple<>, typename Settings = std::tuple<>>
struct StepManifest
{
    using Step = Step_;
    using Wire = Wire_;
    using Eligibility = bool (*)(const Wire &);

    const char * name;
    UInt64 name_introduced_in = 0;
    Formats formats{};
    Settings setting_entries{};
    Eligibility full_digest_eligible = &Eligible::always<Wire>;
    Eligibility logical_digest_eligible = &Eligible::always<Wire>;
    bool custom = false;

    constexpr explicit StepManifest(const char * name_) : name(name_) { }

    constexpr StepManifest(
        const char * name_,
        UInt64 name_introduced_in_,
        Formats formats_,
        Settings setting_entries_,
        Eligibility full_digest_eligible_,
        Eligibility logical_digest_eligible_,
        bool custom_)
        : name(name_)
        , name_introduced_in(name_introduced_in_)
        , formats(formats_)
        , setting_entries(setting_entries_)
        , full_digest_eligible(full_digest_eligible_)
        , logical_digest_eligible(logical_digest_eligible_)
        , custom(custom_)
    {
    }

    /// The plan version that added this serialization name.
    constexpr StepManifest nameIntroducedIn(UInt64 version) const
    {
        StepManifest copy = *this;
        copy.name_introduced_in = version;
        return copy;
    }

    /// Payload format 1. Its plan version is the framed base version, or the name's version when
    /// that is higher, so it is not declared.
    template <typename... F>
    constexpr auto baseFormat(F... fields) const
    {
        static_assert(std::tuple_size_v<Formats> == 0, "the base format is declared once, before any appended format");
        static_assert((std::is_same_v<typename F::Wire, Wire> && ...), "every field must belong to the manifest's wire struct");
        using NewFormats = std::tuple<WireFormat<F...>>;
        return StepManifest<Step, Wire, NewFormats, Settings>(
            name, name_introduced_in, NewFormats{WireFormat<F...>{0, std::tuple<F...>{fields...}}},
            setting_entries, full_digest_eligible, logical_digest_eligible, custom);
    }

    /// The next payload format, appended after the previous one. Older readers skip its bytes;
    /// when any of its values differs from its initializer, the plan requires a reader at or above
    /// `introduced`.
    template <typename... F>
    constexpr auto appendFormat(IntroducedIn introduced, F... fields) const
    {
        static_assert(std::tuple_size_v<Formats> >= 1, "declare the base format before an appended one");
        static_assert((std::is_same_v<typename F::Wire, Wire> && ...), "every field must belong to the manifest's wire struct");
        auto new_formats = std::tuple_cat(formats, std::tuple<WireFormat<F...>>{WireFormat<F...>{introduced.version, std::tuple<F...>{fields...}}});
        return StepManifest<Step, Wire, decltype(new_formats), Settings>(
            name, name_introduced_in, new_formats, setting_entries, full_digest_eligible, logical_digest_eligible, custom);
    }

    /// The values the step sends through the settings channel.
    template <typename... S>
    constexpr auto settings(S... entries) const
    {
        static_assert(std::tuple_size_v<Settings> == 0, "the settings are declared once");
        static_assert((std::is_same_v<typename S::Wire, Wire> && ...), "every setting must belong to the manifest's wire struct");
        using NewSettings = std::tuple<S...>;
        return StepManifest<Step, Wire, Formats, NewSettings>(
            name, name_introduced_in, formats, NewSettings{entries...}, full_digest_eligible, logical_digest_eligible, custom);
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

    /// The step keeps its hand-written serializer, settings and requirements. The manifest then
    /// declares the name, the appended formats with their plan versions and the eligibility only.
    constexpr StepManifest customSerialization() const
    {
        StepManifest copy = *this;
        copy.custom = true;
        return copy;
    }

    static constexpr size_t formatCount() { return std::tuple_size_v<Formats>; }

    /// The plan version of payload format `ordinal`, 1 being the base.
    template <size_t Ordinal>
    constexpr UInt64 formatIntroducedIn() const
    {
        if constexpr (Ordinal == 1)
            return std::max<UInt64>(DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE, name_introduced_in);
        else
            return std::get<Ordinal - 1>(formats).introduced_in;
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

/// Every binding of a manifest, fields of all formats and then settings, as one tuple.
template <typename Manifest>
constexpr auto allBindings(const Manifest & manifest)
{
    auto fields = std::apply([](const auto &... format) { return std::tuple_cat(format.fields...); }, manifest.formats);
    return std::tuple_cat(fields, manifest.setting_entries);
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

template <typename T>
inline constexpr bool is_codec
    = std::is_same_v<T, SortDescription> || std::is_same_v<T, ActionsDAG> || std::is_same_v<T, AggregateDescriptions>
    || std::is_same_v<T, TableExpressionModifiers::Rational>;

[[noreturn]] void throwCannotParse(const char * what);

/// A length or count that is about to be allocated must fit into the bytes that remain. Framed
/// payloads are read from memory, so what remains is exactly the payload's tail.
inline void checkFitsRemaining(UInt64 count, ReadBuffer & in, const char * what)
{
    if (count > in.available())
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
    if constexpr (std::is_same_v<T, bool>)
        writeBinary(UInt8(value ? 1 : 0), out);
    else if constexpr (std::is_same_v<T, SortDescription>)
        serializeSortDescription(value, out);
    else if constexpr (std::is_same_v<T, ActionsDAG>)
        value.serialize(out, ctx.registry);
    else if constexpr (std::is_same_v<T, AggregateDescriptions>)
        serializeAggregateDescriptions(value, out);
    else if constexpr (std::is_same_v<T, TableExpressionModifiers::Rational>)
        serializeRational(value, out);
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
        static_assert(sizeof(T) == 0, "this type has no wire encoding; use a supported type, a codec type, or make the step custom");
}

template <typename T>
void read(T & value, IQueryPlanStep::Deserialization & ctx)
{
    auto & in = ctx.in;
    if constexpr (std::is_same_v<T, bool>)
    {
        UInt8 byte = 0;
        readBinary(byte, in);
        if (byte > 1)
            WireDetail::throwCannotParse("a bool must be 0 or 1");
        value = byte == 1;
    }
    else if constexpr (std::is_same_v<T, SortDescription>)
        deserializeSortDescription(value, in);
    else if constexpr (std::is_same_v<T, ActionsDAG>)
        value = ActionsDAG::deserialize(in, ctx.registry, ctx.context, ctx.max_type_complexity);
    else if constexpr (std::is_same_v<T, AggregateDescriptions>)
        deserializeAggregateDescriptions(value, in, ctx.max_type_complexity);
    else if constexpr (std::is_same_v<T, TableExpressionModifiers::Rational>)
        value = deserializeRational(in);
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
        static_assert(sizeof(T) == 0, "this type has no wire encoding; use a supported type, a codec type, or make the step custom");
}

/// The canonical name of a member type, for the baseline.
template <typename T>
String typeName()
{
    if constexpr (std::is_same_v<T, bool>)
        return "bool";
    else if constexpr (std::is_same_v<T, SortDescription>)
        return "SortDescription";
    else if constexpr (std::is_same_v<T, ActionsDAG>)
        return "ActionsDAG";
    else if constexpr (std::is_same_v<T, AggregateDescriptions>)
        return "AggregateDescriptions";
    else if constexpr (std::is_same_v<T, TableExpressionModifiers::Rational>)
        return "Rational";
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

/// Whether a value is what a reader reconstructs when the value is not on the wire.
template <typename T>
bool atInitializer(const T & value, const T & initializer)
{
    if constexpr (WireDetail::is_optional<T>)
    {
        if (value.has_value() != initializer.has_value())
            return false;
        if (!value.has_value())
            return true;
        return atInitializer(*value, *initializer);
    }
    else
    {
        static_assert(requires { value == initializer; },
            "a member of an appended format must be comparable, so the writer can tell a value an old reader needs");
        return value == initializer;
    }
}

}


/// The generated operations over a manifest.

/// Writes the payload: every format the target stream version knows, in order. Lowers
/// `ctx.step_format_version` to the last format written, and raises the reader requirement to a
/// format's plan version when one of its values differs from its initializer, because a reader that
/// skips that format reconstructs the initializer.
template <typename Manifest>
void writeManifestPayload(const Manifest & manifest, const typename Manifest::Wire & wire, IQueryPlanStep::Serialization & ctx)
{
    using Wire = typename Manifest::Wire;
    static const Wire initializers{};

    UInt64 written = 0;
    [&]<size_t... I>(std::index_sequence<I...>)
    {
        (
            [&]
            {
                constexpr size_t ordinal = I + 1;
                const auto & format = std::get<I>(manifest.formats);
                const UInt64 introduced_in = manifest.template formatIntroducedIn<ordinal>();
                if constexpr (ordinal > 1)
                {
                    bool at_initializers = true;
                    WireDetail::forEach(format.fields, [&](const auto & field)
                    {
                        at_initializers = at_initializers && WireEncoding::atInitializer(wire.*field.member, initializers.*field.member);
                    });
                    if (!at_initializers)
                        ctx.requireReaderVersion(introduced_in);
                    if (introduced_in > ctx.version)
                        return;
                }
                WireDetail::forEach(format.fields, [&](const auto & field) { WireEncoding::write(wire.*field.member, ctx); });
                written = ordinal;
            }(),
            ...);
    }(std::make_index_sequence<Manifest::formatCount()>{});

    ctx.step_format_version = written;
}

/// Reads the payload into a default-constructed wire struct: the settings first, from the node's
/// settings entries, then the formats up to the one the outline names. A format above the ones
/// this binary knows is left to the frame, which skips it by the payload size.
template <typename Manifest>
typename Manifest::Wire readManifestPayload(const Manifest & manifest, IQueryPlanStep::Deserialization & ctx)
{
    using Wire = typename Manifest::Wire;
    Wire wire{};

    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        wire.*entry.member = static_cast<Value>(ctx.settings[*entry.setting]);
    });

    [&]<size_t... I>(std::index_sequence<I...>)
    {
        (
            [&]
            {
                constexpr size_t ordinal = I + 1;
                if (ordinal > ctx.step_format_version)
                    return;
                WireDetail::forEach(std::get<I>(manifest.formats).fields, [&](const auto & field) { WireEncoding::read(wire.*field.member, ctx); });
            }(),
            ...);
    }(std::make_index_sequence<Manifest::formatCount()>{});

    return wire;
}

/// Fills the settings channel: an entry is written exactly when the value differs from the
/// registered default. The frame raises the reader requirement for a setting a target does not know.
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

/// Calls `visitor(name, field_class, value)` for every payload field of every format and every
/// setting, in declaration order. This is the input of the digests: the full digest takes every
/// entry, the logical digest the `Logical` ones.
template <typename Manifest, typename Visitor>
void forEachWireEntry(const Manifest & manifest, const typename Manifest::Wire & wire, Visitor && visitor)
{
    WireDetail::forEach(manifest.formats, [&](const auto & format)
    {
        WireDetail::forEach(format.fields, [&](const auto & field) { visitor(field.name, field.field_class, wire.*field.member); });
    });
    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        visitor(QueryPlanSerializationSettings::settingName(*entry.setting), entry.field_class, wire.*entry.member);
    });
}

/// The registry entry a manifest describes: the name's plan version and one appended payload
/// format per `appendFormat`. An appended format needs nothing from a reader that skips it, so its
/// static requirement is none; the value-dependent one is raised while writing.
template <typename Manifest>
QueryPlanStepRegistry::StepSerializationInfo manifestRegistryInfo(const Manifest & manifest)
{
    QueryPlanStepRegistry::StepSerializationInfo info;
    info.introduced_in_plan_version = manifest.name_introduced_in;
    info.has_wire_struct = !manifest.custom;
    for (UInt64 ordinal = 2; ordinal <= Manifest::formatCount(); ++ordinal)
        info.payload_formats[ordinal] = {QueryPlanStepRegistry::PayloadChange::Append, /*min_plan_version=*/0};
    return info;
}

/// The manifest in a canonical text form, one line per node, for the baseline test.
template <typename Manifest>
String describeManifest(const Manifest & manifest)
{
    using Wire = typename Manifest::Wire;
    WriteBufferFromOwnString out;
    out << "name " << manifest.name << " introduced_in " << manifest.name_introduced_in
        << (manifest.custom ? " custom" : "")
        << " full_digest " << (manifest.full_digest_eligible == &Eligible::always<Wire> ? "always" : manifest.full_digest_eligible == &Eligible::never<Wire> ? "never" : "predicate")
        << " logical_digest " << (manifest.logical_digest_eligible == &Eligible::always<Wire> ? "always" : manifest.logical_digest_eligible == &Eligible::never<Wire> ? "never" : "predicate")
        << "\n";

    [&]<size_t... I>(std::index_sequence<I...>)
    {
        (
            [&]
            {
                constexpr size_t ordinal = I + 1;
                out << "format " << ordinal << " introduced_in " << manifest.template formatIntroducedIn<ordinal>() << "\n";
                WireDetail::forEach(std::get<I>(manifest.formats).fields, [&](const auto & field)
                {
                    using Value = typename std::remove_cvref_t<decltype(field)>::Value;
                    out << "  field " << field.name << " " << (field.field_class == WireFieldClass::Logical ? "Logical" : "Physical")
                        << " " << WireEncoding::typeName<Value>() << "\n";
                });
            }(),
            ...);
    }(std::make_index_sequence<Manifest::formatCount()>{});

    WireDetail::forEach(manifest.setting_entries, [&](const auto & entry)
    {
        using Value = typename std::remove_cvref_t<decltype(entry)>::Value;
        out << "  setting " << QueryPlanSerializationSettings::settingName(*entry.setting) << " "
            << (entry.field_class == WireFieldClass::Logical ? "Logical" : "Physical") << " " << WireEncoding::typeName<Value>() << "\n";
    });

    if constexpr (Manifest::formatCount() > 0)
    {
        /// The initializers, as the payload of a default-constructed wire struct: what a reader
        /// reconstructs for every value that is not on the wire. Every format is written, also one
        /// above the version this binary speaks, so the initializers of every appended format are pinned.
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

/// Registers a step by its manifest: the registry entry is derived and the description of the
/// declaration goes with it. Fails to compile unless the manifest binds every member of the wire
/// struct exactly once.
template <const auto & manifest>
void registerManifest(QueryPlanStepRegistry & registry, QueryPlanStepRegistry::StepCreateFunction create)
{
    static_assert(manifestCoversWire(manifest), "the manifest must bind every member of its wire struct exactly once");
    registry.registerStep(manifest.name, std::move(create), manifestRegistryInfo(manifest), describeManifest(manifest));
}

/// Whether a stream is framed, and the generated path applies, or older, and the hand-written one does.
inline bool usesManifest(UInt64 stream_version)
{
    return stream_version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
}

}
