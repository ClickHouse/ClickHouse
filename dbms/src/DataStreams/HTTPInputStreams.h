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
    using ReadBufferUniquePtr = std::unique_ptr<ReadBuffer>;

    ReadBufferUniquePtr in;
    ReadBufferUniquePtr in_maybe_compressed;
    ReadBufferUniquePtr in_maybe_internal_compressed;

    HTTPInputStreams(Context & context, HTTPServerRequest & request, HTMLForm & from);

    ReadBufferUniquePtr createRawInBuffer(HTTPServerRequest & request) const;
    ReadBufferUniquePtr createCompressedBuffer(HTTPServerRequest & request, ReadBufferUniquePtr & raw_buffer) const;
    ReadBufferUniquePtr createInternalCompressedBuffer(HTMLForm & params, ReadBufferUniquePtr & http_maybe_encoding_buffer) const;
};

}
