#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>

#include <Columns/ColumnsNumber.h>
#include <Common/escapeForFileName.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPRequest.h>

#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CatBoostLibraryBridgeHelper::CatBoostLibraryBridgeHelper(
        ContextPtr context_,
        std::optional<String> model_path_,
        std::optional<String> library_path_)
    : LibraryBridgeHelper(context_->getGlobalContext())
    , model_path(model_path_)
    , library_path(library_path_)
{
}

Poco::URI CatBoostLibraryBridgeHelper::getPingURI() const
{
    auto uri = createBaseURI();
    uri.setPath(PING_HANDLER);
    return uri;
}

Poco::URI CatBoostLibraryBridgeHelper::getMainURI() const
{
    auto uri = createBaseURI();
    uri.setPath(MAIN_HANDLER);
    return uri;
}


Poco::URI CatBoostLibraryBridgeHelper::createRequestURI(const String & method) const
{
    auto uri = getMainURI();
    uri.addQueryParameter("version", std::to_string(LIBRARY_BRIDGE_PROTOCOL_VERSION));
    uri.addQueryParameter("method", method);
    return uri;
}

bool CatBoostLibraryBridgeHelper::bridgeHandShake()
{
    String result;
    try
    {
        auto buf = BuilderRWBufferFromHTTP(getPingURI())
                       .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                       .withTimeouts(http_timeouts)
                       .create(credentials);

        readString(result, *buf);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return false;
    }

    if (result != "1")
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Unexpected message from library bridge: {}. "
                        "Check that bridge and server have the same version.", result);

    return true;
}

ExternalModelInfos CatBoostLibraryBridgeHelper::listModels()
{
    startBridgeSync();

    auto buf = BuilderRWBufferFromHTTP(createRequestURI(CATBOOST_LIST_METHOD))
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(http_timeouts)
                   .create(credentials);

    ExternalModelInfos result;

    UInt64 num_rows;
    readIntBinary(num_rows, *buf);

    for (UInt64 i = 0; i < num_rows; ++i)
    {
        ExternalModelInfo info;

        readStringBinary(info.model_path, *buf);
        readStringBinary(info.model_type, *buf);

        UInt64 t;
        readIntBinary(t, *buf);
        info.loading_start_time = std::chrono::system_clock::from_time_t(t);

        readIntBinary(t, *buf);
        info.loading_duration = std::chrono::milliseconds(t);

        result.push_back(info);
    }

    return result;
}

void CatBoostLibraryBridgeHelper::removeModel()
{
    startBridgeSync();

    assert(model_path);

    auto buf = BuilderRWBufferFromHTTP(createRequestURI(CATBOOST_REMOVEMODEL_METHOD))
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(http_timeouts)
                   .withOutCallback(
                       [this](std::ostream & os)
                       {
                           os << "model_path=" << escapeForFileName(*model_path);
                       })
                   .create(credentials);

    String result;
    readStringBinary(result, *buf);
    assert(result == "1");
}

void CatBoostLibraryBridgeHelper::removeAllModels()
{
    startBridgeSync();

    auto buf = BuilderRWBufferFromHTTP(createRequestURI(CATBOOST_REMOVEALLMODELS_METHOD))
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(http_timeouts)
                   .create(credentials);

    String result;
    readStringBinary(result, *buf);
    assert(result == "1");
}

size_t CatBoostLibraryBridgeHelper::getTreeCount()
{
    startBridgeSync();

    assert(model_path && library_path);

    auto buf = BuilderRWBufferFromHTTP(createRequestURI(CATBOOST_GETTREECOUNT_METHOD))
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(http_timeouts)
                   .withOutCallback(
                        [this](std::ostream & os)
                        {
                           os << "library_path=" << escapeForFileName(*library_path) << "&";
                           os << "model_path=" << escapeForFileName(*model_path);
                        })
                   .create(credentials);

    size_t result;
    readIntBinary(result, *buf);
    return result;
}

ColumnPtr CatBoostLibraryBridgeHelper::evaluate(const ColumnsWithTypeAndName & columns)
{
    startBridgeSync();

    WriteBufferFromOwnString string_write_buf;
    Block block(columns);
    NativeWriter serializer(string_write_buf, /*client_revision*/ 0, block);
    serializer.write(block);

    assert(model_path);

    auto buf = BuilderRWBufferFromHTTP(createRequestURI(CATBOOST_LIB_EVALUATE_METHOD))
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(http_timeouts)
                   .withOutCallback(
                       [this, serialized = string_write_buf.str()](std::ostream & os)
                       {
                           os << "model_path=" << escapeForFileName(*model_path) << "&";
                           os << "data=" << escapeForFileName(serialized);
                       })
                   .create(credentials);

    NativeReader deserializer(*buf, /*server_revision*/ 0);
    Block block_read = deserializer.read();

    return block_read.getColumns()[0];
}

}
