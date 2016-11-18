#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>
//#include <Poco/Net/HTTPRequest.h> // HTTP_GET

namespace DB
{

HTTPDictionarySource::HTTPDictionarySource(const DictionaryStructure & dict_struct_, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	host{config.getString(config_prefix + ".host")},
	port{std::stoi(config.getString(config_prefix + ".port"))},
	path{config.getString(config_prefix + ".path")},
	method{config.getString(config_prefix + ".method")},
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
	auto in_ptr = std::make_unique<ReadBufferFromHTTP>(host, port, path, ReadBufferFromHTTP::Params(), method);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
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
