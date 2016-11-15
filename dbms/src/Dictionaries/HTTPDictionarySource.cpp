#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/RemoteReadBuffer.h>

namespace DB
{


decltype(HTTPDictionarySource::max_block_size) HTTPDictionarySource::max_block_size;

HTTPDictionarySource::HTTPDictionarySource(const std::string & url, const std::string & format, Block & sample_block, const Context & context)
	: url{url}, format{format}, sample_block{sample_block}, context(context)
{
		last_modification = LocalDateTime {std::time(nullptr)};
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other)
	: url{other.url},
	  format{other.format},
	  sample_block{other.sample_block}, context(other.context),
	  last_modification{other.last_modification}
{
}


BlockInputStreamPtr HTTPDictionarySource::loadAll()
{

	//RemoteReadBuffer
	
/*
	ReadBufferFromHTTP::Params params =
	{
		//{"endpoint", getEndpointId(location.name)},
		{"compress", "false"},
		{"query", query}
	};

	ReadBufferFromHTTP in{location.host, location.port, params};
*/

std::cerr << " http go " << url << "\n";

	auto in_ptr = std::make_unique<RemoteReadBuffer>(url, 80, "/");
	auto stream = context.getInputFormat( format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBufferBlockInputStream>(stream, std::move(in_ptr));

	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

BlockInputStreamPtr HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

bool HTTPDictionarySource::isModified() const
{
	return getLastModification() > last_modification;
}

bool HTTPDictionarySource::supportsSelectiveLoad() const
{
	return false;
}

DictionarySourcePtr HTTPDictionarySource::clone() const
{
	return std::make_unique<HTTPDictionarySource>(*this);
}

std::string HTTPDictionarySource::toString() const
{
	return "HTTP: " + url;
}

LocalDateTime HTTPDictionarySource::getLastModification() const
{
	return last_modification;
}


}
