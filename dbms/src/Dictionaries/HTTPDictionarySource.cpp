#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadWriteBufferFromHTTP.h>

namespace DB
{

HTTPDictionarySource::HTTPDictionarySource(const DictionaryStructure & dict_struct_, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	host{config.getString(config_prefix + ".host", "::1")},
	port{std::stoi(config.getString(config_prefix + ".port", "80"))},
	path{config.getString(config_prefix + ".path", "")},
	//method{config.getString(config_prefix + ".method", "")},
	format{config.getString(config_prefix + ".format")},
	sample_block{sample_block},
	context(context)
{
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other) :
	dict_struct{other.dict_struct},
	host{other.host},
	port{other.port},
	path{other.path},
	format{other.format},
	sample_block{other.sample_block},
	context(other.context)
{
}

BlockInputStreamPtr HTTPDictionarySource::loadAll()
{
	LOG_TRACE(log, "loadAll " + toString());
	auto in_ptr = std::make_unique<ReadBufferFromHTTP>(host, port, path, ReadBufferFromHTTP::Params(), Poco::Net::HTTPRequest::HTTP_GET);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBufferBlockInputStream>(stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	LOG_TRACE(log, "loadIds " + toString());
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};

	HTTPLocation http_location;
	http_location.host = host;
	http_location.port = port;
	http_location.path = path;
	//http_location.method = method;
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(http_location);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));

}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	LOG_TRACE(log, "loadKeys " + toString());
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

bool HTTPDictionarySource::isModified() const
{
	return true;
}

bool HTTPDictionarySource::supportsSelectiveLoad() const
{
	return true;
}

DictionarySourcePtr HTTPDictionarySource::clone() const
{
	return std::make_unique<HTTPDictionarySource>(*this);
}

std::string HTTPDictionarySource::toString() const
{
	return "http://" + host + ":" + std::to_string(port) + "/" + path;
}

}
