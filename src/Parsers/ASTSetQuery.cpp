#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

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
            /// Write "value" key and the field as a JSON object via writeFieldValue.
            /// We use a trick: writeFieldValue writes ,"key":{field_json},
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
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto change_obj = arr->getObject(i);
            SettingChange change;
            change.name = change_obj->getValue<String>("name");
            auto value_obj = change_obj->getObject("value");
            change.value = JSONObjectReader::readFieldFromObject(*value_obj);
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
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto param_obj = arr->getObject(i);
            query_parameters.emplace_back(
                param_obj->getValue<String>("name"),
                param_obj->getValue<String>("value"));
        }
    }
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
