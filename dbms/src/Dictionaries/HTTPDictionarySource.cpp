#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/RemoteReadBuffer.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{


decltype(HTTPDictionarySource::max_block_size) HTTPDictionarySource::max_block_size;

HTTPDictionarySource::HTTPDictionarySource(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Block & sample_block, const Context & context) :
	host{config.getString(config_prefix + ".host")},
	port{std::stoi(config.getString(config_prefix + ".port"))},
	path{config.getString(config_prefix + ".path")},
	format{config.getString(config_prefix + ".format")},
	sample_block{sample_block},
	context(context)
{
		last_modification = std::time(nullptr);

}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other) :
	host{other.host},
	port{other.port},
	path{other.path},
	format{other.format},
	sample_block{other.sample_block}, context(other.context),
	last_modification{other.last_modification}
{
}

BlockInputStreamPtr HTTPDictionarySource::loadAll()
{
	auto in_ptr = std::make_unique<ReadBufferFromHTTP>(host, port, path, params, Poco::Net::HTTPRequest::HTTP_GET);
	auto stream = context.getInputFormat( format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBufferBlockInputStream>(stream, std::move(in_ptr));
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
	return "http://" + host + ":" + std::to_string(port) + "/" + path;
}

LocalDateTime HTTPDictionarySource::getLastModification() const
{
	return last_modification;
}

}
