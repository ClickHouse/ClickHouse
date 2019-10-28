#include <DataStreams/IBlockInputStream.h>
#include <IO/CompressionMethod.h>

namespace DB
{

class IBlockInputStreamWithCompression : public IBlockInputStream
{
protected:
	template <class TReadBuffer>
	std::unique_ptr<ReadBuffer> GetBuffer(const String & uri, const DB::CompressionMethod method) {
		if (method == DB::CompressionMethod::Gzip) {
            auto read_buf = std::make_unique<TReadBuffer>(uri);
            return std::make_unique<ZlibInflatingReadBuffer>(std::move(read_buf), DB::CompressionMethod::Gzip);
        }
        return std::make_unique<TReadBuffer>(uri);
    };
};
}