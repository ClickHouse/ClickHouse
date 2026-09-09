#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFromJSON.h>

#include <Core/SettingsSecrets.h>
#include <Databases/DataLake/DataLakeConstants.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/formatSettingName.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
#include <Storages/ObjectStorageQueue/AzureQueue_fwd.h>
#include <Storages/ObjectStorageQueue/S3Queue_fwd.h>
#include <Storages/RabbitMQ/RabbitMQ_fwd.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Each engine namespace declares its own identical `ValueMaskingFunc` alias, hence the spelled-out
/// type. Unrelated to `CoreSettings::ValueMaskingFunc`, which rewrites a value string in place.
using EngineSettingsToHide = std::unordered_map<String, std::function<std::string(const Field &)>>;

/// The table and database engine settings whose value is a secret, and how each one is masked.
///
/// Every engine's map is consulted whatever the engine of the statement being formatted, because
/// `FormatStateStacked::create_engine_name` is only set when a `SETTINGS` clause is formatted as part
/// of `ENGINE = ...`. Gating on it printed the value of
/// `ALTER TABLE t MODIFY SETTING kafka_sasl_password = '...'` in cleartext. The setting names are
/// engine-prefixed, so there is nothing for a different engine to collide with.
///
/// `formatImpl` and `hasSecretParts` both read this list, so they cannot disagree on what is secret.
static std::array<const EngineSettingsToHide *, 6> engineSettingsToHide()
{
    return {
        &DataLake::SETTINGS_TO_HIDE,
        &RabbitMQ::SETTINGS_TO_HIDE,
        &NATS::SETTINGS_TO_HIDE,
        &Kafka::SETTINGS_TO_HIDE,
        &AzureQueue::SETTINGS_TO_HIDE,
        &S3Queue::SETTINGS_TO_HIDE,
    };
}

/// Renders a change whose value is a secret as the SQL text that hides it, and returns `nullopt` for
/// a change that carries none. `formatImpl` and `hasSecretParts` both go through this, so they cannot
/// disagree on what is secret.
static std::optional<String> renderSecretChangeValue(const SettingChange & change)
{
    if (auto masked = CoreSettings::renderSecretSettingValue(change.name, change.value))
        return masked;

    for (const auto * settings_to_hide : engineSettingsToHide())
    {
        auto it = settings_to_hide->find(change.name);
        if (it != settings_to_hide->end())
            return it->second(change.value);
    }

    return {};
}


class FieldVisitorToSetting : public StaticVisitor<String>
{
public:
    template <class T>
    String operator() (const T & x) const
    {
        FieldVisitorToString visitor;
        return visitor(x);
    }

    String operator() (const Map & x) const
    {
        WriteBufferFromOwnString wb;

        wb << '{';

        auto it = x.begin();
        while (it != x.end())
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(*this, *it);
            ++it;
        }
        wb << '}';

        return wb.str();
    }

    String operator() (const Tuple & x) const
    {
        WriteBufferFromOwnString wb;

        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ":";
            wb << applyVisitor(*this, *it);
        }

        return wb.str();
    }
};


void ASTSetQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// None of the members below is a child, so the default implementation does not see them.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 112, "If members were added to ASTSetQuery, hash them here unless they are purely cosmetic.");

    /// Not cosmetic: `formatImpl` prints the `SET` keyword only for a standalone query.
    hash_state.update(is_standalone);

    /// The three lists hold different kinds of entry, and a query parameter is stored with its
    /// `param_` prefix removed. Their sizes are hashed so that one list cannot be mistaken for
    /// another, and every value is length-prefixed so that neighbouring entries cannot be read as
    /// one: `param_x = 0` must not stream the same bytes as `x` set to a UInt64 that happens to
    /// spell the character `0`.
    hash_state.update(changes.size());
    hash_state.update(default_settings.size());
    hash_state.update(query_parameters.size());

    for (const auto & change : changes)
    {
        hash_state.update(change.name.size());
        hash_state.update(change.name);
        hash_state.update(change.shorthand);
        applyVisitor(FieldVisitorHash(hash_state), change.value);
    }

    /// `x = DEFAULT` resets a setting and is not recorded in `changes`.
    for (const auto & setting_name : default_settings)
    {
        hash_state.update(setting_name.size());
        hash_state.update(setting_name);
    }

    for (const auto & [name, value] : query_parameters)
    {
        hash_state.update(name.size());
        hash_state.update(name);
        hash_state.update(value.size());
        hash_state.update(value);
    }

    /// This override used to skip the base implementation, leaving out the node's own identity.
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTSetQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (is_standalone)
        ostr << "SET ";

    bool first = true;

    for (const auto & change : changes)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, ostr);

        /// The valueless form has to survive a format/parse round trip: written back as
        /// `name = true` it would be accepted for a setting of any type, which is exactly what the
        /// shorthand check exists to prevent. The value is Bool `true` and never secret.
        ///
        /// Only elide the value when it really is `true`. The AST JSON dialect can pair the
        /// shorthand flag with any other value, and such a change is rejected by
        /// `BaseSettings::checkShorthandChange` - but that happens after the query is logged, so the
        /// formatter must not print a bare name for a change that carries something else.
        if (change.shorthand && change.value == Field(true))
            continue;

        std::optional<String> masked;
        if (!format.show_secrets)
            masked = renderSecretChangeValue(change);

        if (masked)
            ostr << " = " << *masked;
        else
            ostr << " = " << applyVisitor(FieldVisitorToSetting(), change.value);
    }

    for (const auto & setting_name : default_settings)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(setting_name, ostr);
        ostr << " = DEFAULT";
    }

    for (const auto & [name, value] : query_parameters)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(QUERY_PARAMETER_NAME_PREFIX + name, ostr);
        ostr << " = " << quoteString(value);
    }
}

void ASTSetQuery::appendColumnName(WriteBuffer & ostr) const
{
    IASTHash hash = getTreeHash(/*ignore_aliases=*/ true);

    writeCString("__settings_", ostr);
    writeText(hash.low64, ostr);
    ostr.write('_');
    writeText(hash.high64, ostr);
}

void ASTSetQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "SetQuery");

    if (is_standalone)
        w.writeBool("is_standalone", true);

    if (!changes.empty())
    {
        w.writeKey("changes");
        auto & o = w.getOut();
        const auto & fs = w.getFormatSettings();
        o << '[';
        for (size_t i = 0; i < changes.size(); ++i)
        {
            if (i > 0) o << ',';
            o << "{\"name\":";
            writeJSONString(changes[i].name, o, fs);
            /// The valueless form has to survive the JSON round trip for the same reason it has to
            /// survive the SQL one (see `formatImpl`): reconstructed as an explicit `name = true` it
            /// would be accepted for a setting of any type, which is what the shorthand check exists
            /// to prevent. The value is Bool `true`, so it carries no information beyond the flag.
            if (changes[i].shorthand)
                o << ",\"shorthand\":true";
            /// Write "value" key and the field as a JSON object via writeFieldValue.
            /// We use a trick: writeFieldValue writes, "key":{field_json},
            /// but since we just wrote {"name":"..." the comma is exactly what we need.
            w.writeFieldValue("value", changes[i].value);
            o << '}';
        }
        o << ']';
    }

    if (!default_settings.empty())
    {
        w.writeKey("default_settings");
        auto & o = w.getOut();
        const auto & fs = w.getFormatSettings();
        o << '[';
        for (size_t i = 0; i < default_settings.size(); ++i)
        {
            if (i > 0) o << ',';
            writeJSONString(default_settings[i], o, fs);
        }
        o << ']';
    }

    if (!query_parameters.empty())
    {
        w.writeKey("query_parameters");
        auto & o = w.getOut();
        const auto & fs = w.getFormatSettings();
        o << '[';
        for (size_t i = 0; i < query_parameters.size(); ++i)
        {
            if (i > 0) o << ',';
            o << "{\"name\":";
            writeJSONString(query_parameters[i].first, o, fs);
            o << ",\"value\":";
            writeJSONString(query_parameters[i].second, o, fs);
            o << '}';
        }
        o << ']';
    }
}

void ASTSetQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    is_standalone = r.getBool("is_standalone");

    if (r.has("changes"))
    {
        auto arr = r.getArray("changes");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'changes' is not an array during AST JSON deserialization");
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto change_obj = arr->getObject(i);
            if (!change_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'changes' array during AST JSON deserialization", i);
            SettingChange change;
            /// Read the name through `JSONObjectReader` so a non-string value is rejected with
            /// `BAD_ARGUMENTS` instead of being coerced (e.g. a number stringified into a setting name).
            JSONObjectReader change_reader(*change_obj);
            change.name = change_reader.getString("name");
            /// Restore the valueless form so `checkShorthandChange` still rejects a non-`Bool`
            /// setting written without a value; otherwise the JSON dialect is a way around it.
            change.shorthand = change_reader.getBool("shorthand");
            auto value_obj = change_obj->getObject("value");
            if (!value_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'value' object at index {} in 'changes' array during AST JSON deserialization", i);
            change.value = JSONObjectReader::readFieldFromObject(*value_obj);
            /// A payload may pair the flag with a value the parser would never produce. That is not
            /// rejected here: deserialization runs before `executeQueryImpl` has an AST to mask with,
            /// so throwing would send the raw JSON text - the value included - down the unmasked
            /// `wipeSensitiveDataAndCutToLength` logging path. The value is kept instead, so
            /// `hasSecretParts` can still see it and `formatImpl` can still hide it, and
            /// `checkShorthandChange` then rejects the setting itself once logging is safe.
            changes.push_back(std::move(change));
        }
    }

    if (r.has("default_settings"))
    {
        default_settings = r.readStringArray("default_settings");
    }

    if (r.has("query_parameters"))
    {
        auto arr = r.getArray("query_parameters");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'query_parameters' is not an array during AST JSON deserialization");
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            /// `query_parameters` is a non-AST array; count each pair against `max_ast_elements` so a
            /// tiny-AST payload cannot carry millions of parameters and allocate/format them unbounded.
            countJSONDeserializationElement();
            auto param_obj = arr->getObject(i);
            if (!param_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'query_parameters' array during AST JSON deserialization", i);
            /// Read both scalars strictly so a non-string name/value is rejected rather than coerced.
            JSONObjectReader param_reader(*param_obj);
            query_parameters.emplace_back(
                param_reader.getString("name"),
                param_reader.getString("value"));
        }
    }
}

bool ASTSetQuery::hasSecretParts() const
{
    return std::any_of(
        changes.begin(), changes.end(), [](const auto & change) { return renderSecretChangeValue(change).has_value(); });
}

}
