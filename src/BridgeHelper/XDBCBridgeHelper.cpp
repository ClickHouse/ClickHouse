#include <BridgeHelper/XDBCBridgeHelper.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
Poco::URI XDBCBridgeHelper<T>::createBaseURI() const
{
    Poco::URI uri;
    uri.setHost(bridge_host);
    uri.setPort(static_cast<uint16_t>(bridge_port));
    uri.setScheme("http");
    uri.addQueryParameter("use_connection_pooling", toString(use_connection_pooling));
    return uri;
}

template <typename T>
bool XDBCBridgeHelper<T>::isSchemaAllowed()
{
    if (!is_schema_allowed.has_value())
    {
        startBridgeSync();

        auto uri = createBaseURI();
        uri.setPath(SCHEMA_ALLOWED_HANDLER);
        uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
        uri.addQueryParameter("connection_string", getConnectionString());
        uri.addQueryParameter("use_connection_pooling", toString(use_connection_pooling));

        auto buf = BuilderRWBufferFromHTTP(uri)
                       .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                       .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                       .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(getContext()->getSettingsRef(), getContext()->getServerSettings()))
                       .withSettings(getContext()->getReadSettings())
                       .create(credentials);

        bool res = false;
        readBoolText(res, *buf);
        is_schema_allowed = res;
    }

    return *is_schema_allowed;
}

template <typename T>
IdentifierQuotingStyle XDBCBridgeHelper<T>::getIdentifierQuotingStyle()
{
    if (!quote_style.has_value())
    {
        startBridgeSync();

        auto uri = createBaseURI();
        uri.setPath(IDENTIFIER_QUOTE_HANDLER);
        uri.addQueryParameter("version", std::to_string(XDBC_BRIDGE_PROTOCOL_VERSION));
        uri.addQueryParameter("connection_string", getConnectionString());
        uri.addQueryParameter("use_connection_pooling", toString(use_connection_pooling));

        auto buf = BuilderRWBufferFromHTTP(uri)
                       .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                       .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                       .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(getContext()->getSettingsRef(), getContext()->getServerSettings()))
                       .withSettings(getContext()->getReadSettings())
                       .create(credentials);

        std::string character;
        readStringBinary(character, *buf);
        if (character.length() > 1)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Failed to parse quoting style from '{}' for service {}",
                character, T::serviceAlias());

        if (character.empty())
            quote_style = IdentifierQuotingStyle::Backticks;
        else if (character[0] == '`')
            quote_style = IdentifierQuotingStyle::Backticks;
        else if (character[0] == '"')
            quote_style = IdentifierQuotingStyle::DoubleQuotes;
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Can not map quote identifier '{}' to enum value", character);
    }

    return *quote_style;
}

template class XDBCBridgeHelper<ODBCBridgeMixin>;
template class XDBCBridgeHelper<JDBCBridgeMixin>;

}
