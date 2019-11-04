#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Common/HTMLForm.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPServerRequest.h>

namespace DB
{

using HTTPServerRequest = Poco::Net::HTTPServerRequest;

struct HTTPInputStreams
{
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<ReadBuffer> in_maybe_compressed;
    std::shared_ptr<ReadBuffer> in_maybe_internal_compressed;

    HTTPInputStreams(Context & context, HTTPServerRequest & request, HTMLForm & from);

    ReadBufferPtr createRawInBuffer(HTTPServerRequest & request) const;
    ReadBufferPtr createCompressedBuffer(HTTPServerRequest & request, ReadBufferPtr & raw_buffer) const;
    ReadBufferPtr createInternalCompressedBuffer(HTMLForm & params, ReadBufferPtr & http_maybe_encoding_buffer) const;
};

}