#include <Parsers/ASTSetQuery.h>

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


void ASTSetQuery::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    for (const auto & change : changes)
    {
        hash_state.update(change.name.size());
        hash_state.update(change.name);
        applyVisitor(FieldVisitorHash(hash_state), change.value);
    }
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

bool ASTSetQuery::hasSecretParts() const
{
    return std::any_of(
        changes.begin(), changes.end(), [](const auto & change) { return renderSecretChangeValue(change).has_value(); });
}

}
