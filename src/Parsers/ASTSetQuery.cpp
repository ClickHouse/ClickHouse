#include <Parsers/ASTSetQuery.h>

#include <Databases/DataLake/DataLakeConstants.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/formatSettingName.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
#include <Storages/ObjectStorageQueue/AzureQueue_fwd.h>
#include <Storages/ObjectStorageQueue/S3Queue_fwd.h>
#include <Storages/RabbitMQ/RabbitMQ_fwd.h>
#include <Poco/Exception.h>
#include <Poco/URI.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

static constexpr std::string_view format_avro_schema_registry_url = "format_avro_schema_registry_url";

namespace DB
{

namespace
{
std::optional<Poco::URI> tryParseURI(const String & uri)
{
    try
    {
        return Poco::URI (uri);
    }
    catch (const Poco::SyntaxException &)
    {
        return std::nullopt;
    }
}
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
    /// None of the members below is a child, so the default implementation does not see them.
    static_assert(sizeof(*this) == 112, "If members were added to ASTSetQuery, hash them here unless they are purely cosmetic.");

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
}

void ASTSetQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked state) const
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

        auto format_if_secret = [&]() -> bool
        {
            CustomType custom;
            if (change.value.tryGet<CustomType>(custom) && custom.isSecret())
            {
                ostr << " = " << custom.toString(/* show_secrets */false);
                return true;
            }

            if (change.name == format_avro_schema_registry_url)
            {
                auto uri_string = change.value.safeGet<String>();
                const auto maybe_uri = tryParseURI(uri_string);
                if (!maybe_uri || maybe_uri->getUserInfo().empty())
                    return false;

                const auto & user_info = maybe_uri->getUserInfo();
                const auto user_name = user_info.substr(0, user_info.find(':'));
                const auto new_user_info = user_name + ":[HIDDEN]";
                uri_string.replace(uri_string.find(user_info),user_info.size(), new_user_info);
                ostr << " = '" << uri_string << "'";
                return true;
            }

            /// Intrinsically secret regardless of engine: DataLakeStorageSettings is shared by the
            /// DataLakeCatalog database engine and the Iceberg*/Paimon*/DeltaLake* table engines.
            /// Matches the ungated check in hasSecretParts().
            if (DataLake::SETTINGS_TO_HIDE.contains(change.name))
            {
                ostr << " = " << DataLake::SETTINGS_TO_HIDE.at(change.name)(change.value);
                return true;
            }
            if (RabbitMQ::TABLE_ENGINE_NAME == state.create_engine_name)
            {
                if (RabbitMQ::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << RabbitMQ::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
            }
            if (NATS::TABLE_ENGINE_NAME == state.create_engine_name)
            {
                if (NATS::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << NATS::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
            }
            if (Kafka::TABLE_ENGINE_NAME == state.create_engine_name)
            {
                if (Kafka::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << Kafka::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
            }
            if (AzureQueue::TABLE_ENGINE_NAME == state.create_engine_name)
            {
                if (AzureQueue::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << AzureQueue::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
            }
            if (S3Queue::TABLE_ENGINE_NAME == state.create_engine_name)
            {
                if (S3Queue::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << S3Queue::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
            }

            return false;
        };

        if (format.show_secrets || !format_if_secret())
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
    for (const auto & change : changes)
    {
        CustomType custom;
        if (change.value.tryGet<CustomType>(custom) && custom.isSecret())
            return true;
        if (DataLake::SETTINGS_TO_HIDE.contains(change.name))
            return true;
        if (RabbitMQ::SETTINGS_TO_HIDE.contains(change.name))
            return true;
        if (NATS::SETTINGS_TO_HIDE.contains(change.name))
            return true;
        if (Kafka::SETTINGS_TO_HIDE.contains(change.name))
            return true;
        if (AzureQueue::SETTINGS_TO_HIDE.contains(change.name))
            return true;
        if (S3Queue::SETTINGS_TO_HIDE.contains(change.name))
            return true;

        if (change.name == format_avro_schema_registry_url)
        {
            const auto maybe_uri = tryParseURI(change.value.safeGet<String>());
            if (maybe_uri && !maybe_uri->getUserInfo().empty())
                return true;
        }
    }
    return false;
}

}
