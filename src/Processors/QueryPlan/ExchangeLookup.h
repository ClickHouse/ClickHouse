#pragma once

#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <memory>

namespace DB
{

class ISink;
class ISource;
class Block;

/// Interface for creating Sink and Source processors by exchange logical id when building query pipeline for
/// distributed query plan fragment. The idea is to store only logical names of exchange streams in the query plan
/// and create actual Sink and Source processors only when the query pipeline is built.
struct IExchangeLookup : boost::noncopyable
{
    virtual ~IExchangeLookup() = default;

    virtual std::shared_ptr<ISink> createSink(const Block & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) = 0;
    virtual std::shared_ptr<ISource> createSource(const Block & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) = 0;
};

using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;

}
