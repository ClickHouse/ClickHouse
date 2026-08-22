#include <Parsers/ASTSetQuery.h>

#include <Databases/DataLake/DataLakeConstants.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/formatSettingName.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
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
    for (const auto & change : changes)
    {
        hash_state.update(change.name.size());
        hash_state.update(change.name);
        applyVisitor(FieldVisitorHash(hash_state), change.value);
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

            if (DataLake::DATABASE_ENGINE_NAME == state.create_engine_name)
            {
                if (DataLake::SETTINGS_TO_HIDE.contains(change.name))
                {
                    ostr << " = " << DataLake::SETTINGS_TO_HIDE.at(change.name)(change.value);
                    return true;
                }
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
