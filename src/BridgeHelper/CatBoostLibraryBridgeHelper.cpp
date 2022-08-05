#include "CatBoostLibraryBridgeHelper.h"

#include <Columns/ColumnsNumber.h>
#include <Common/escapeForFileName.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int LOGICAL_ERROR;
}

CatBoostLibraryBridgeHelper::CatBoostLibraryBridgeHelper(
        ContextPtr context_,
        const String & library_path_,
        const String & model_path_)
    : LibraryBridgeHelper(context_->getGlobalContext())
    , library_path(library_path_)
    , model_path(model_path_)
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
        ReadWriteBufferFromHTTP buf(getPingURI(), Poco::Net::HTTPRequest::HTTP_GET, {}, http_timeouts, credentials);
        readString(result, buf);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return false;
    }

    if (result != "1")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected message from library bridge: {}. Check that bridge and server have the same version.", result);

    return true;
}

size_t CatBoostLibraryBridgeHelper::getTreeCount()
{
    startBridgeSync();

    ReadWriteBufferFromHTTP buf(
        createRequestURI(CATBOOST_GETTREECOUNT_METHOD),
        Poco::Net::HTTPRequest::HTTP_POST,
        [this](std::ostream & os)
        {
            os << "library_path=" << escapeForFileName(library_path) << "&";
            os << "model_path=" << escapeForFileName(model_path);
        },
        http_timeouts, credentials);

    size_t res;
    readIntBinary(res, buf);
    return res;
}

ColumnPtr CatBoostLibraryBridgeHelper::evaluate(const ColumnsWithTypeAndName & columns)
{
    startBridgeSync();

    WriteBufferFromOwnString string_write_buf;
    Block block(columns);
    NativeWriter native_writer(string_write_buf, 0, block);
    native_writer.write(block);

    ReadWriteBufferFromHTTP buf(
        createRequestURI(CATBOOST_LIB_EVALUATE_METHOD),
        Poco::Net::HTTPRequest::HTTP_POST,
        [this, serialized = string_write_buf.str()](std::ostream & os)
        {
            os << "model_path=" << escapeForFileName(model_path) << "&";
            os << "data=" << serialized;
        },
        http_timeouts, credentials);

    String res;
    readStringBinary(res, buf);
    ReadBufferFromString string_read_buf(res);
    NativeReader native_reader(string_read_buf, 0);
    Block block_read = native_reader.read();

    return block_read.getColumns()[0];
}

}
