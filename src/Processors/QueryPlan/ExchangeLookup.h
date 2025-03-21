#pragma once

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

class ISink;
class ISource;
class Block;

/// Interface for creating Sink and Source processors by exchange logical id.
struct IExchangeLookup : boost::noncopyable
{
    virtual ~IExchangeLookup() = default;

    virtual std::shared_ptr<ISink> createSink(const Block & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) = 0;
    virtual std::shared_ptr<ISource> createSource(const Block & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) = 0;
};

using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;

}
