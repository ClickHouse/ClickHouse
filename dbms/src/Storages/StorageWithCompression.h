#pragma once
//#include <Common/config.h>

#include <Storages/IStorage.h>
#include <IO/CompressionMethod.h>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

class StorageWithCompression: public IStorage 
{
protected:

	StorageWithCompression(const String & uri_)
	: uri(uri_) {}

	DB::CompressionMethod GetCompressionMethod() {
		if (boost::algorithm::ends_with(uri, ".gz") /* something else */ ) {
            return DB::CompressionMethod::Gzip;
        } else {
            return DB::CompressionMethod::None;
        }
	}

	String uri;
};
}