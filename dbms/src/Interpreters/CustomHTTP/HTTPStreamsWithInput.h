#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPServerRequest.h>

namespace DB
{

using HTTPServerRequest = Poco::Net::HTTPServerRequest;

struct HTTPStreamsWithInput
{
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<ReadBuffer> in_maybe_compressed;
    std::shared_ptr<ReadBuffer> in_maybe_internal_compressed;

    HTTPStreamsWithInput(HTTPServerRequest & request, HTMLForm & from);

    void attachSettings(Context & context, Settings & settings, HTTPServerRequest & request);

    ReadBufferPtr createRawInBuffer(HTTPServerRequest & request) const;
    ReadBufferPtr createCompressedBuffer(HTTPServerRequest & request, ReadBufferPtr & raw_buffer) const;
    ReadBufferPtr createInternalCompressedBuffer(HTMLForm & params, ReadBufferPtr & http_maybe_encoding_buffer) const;
};

}