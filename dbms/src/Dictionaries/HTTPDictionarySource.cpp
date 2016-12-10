#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <Poco/Net/HTTPRequest.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>
#include <DB/IO/ReadWriteBufferFromHTTP.h>

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Dictionaries/ExecutableDictionarySource.h> // idsToBuffer, columnsToBuffer

namespace DB
{

static const size_t max_block_size = 8192;


HTTPDictionarySource::HTTPDictionarySource(const DictionaryStructure & dict_struct_,
		const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	url{config.getString(config_prefix + ".url", "")},
	format{config.getString(config_prefix + ".format")},
	sample_block{sample_block},
	context(context)
{
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other) :
	dict_struct{other.dict_struct},
	url{other.url},
	sample_block{other.sample_block},
	context(other.context)
{
}

BlockInputStreamPtr HTTPDictionarySource::loadAll()
{
	LOG_TRACE(log, "loadAll " + toString());
	Poco::URI uri(url);
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_GET);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	LOG_TRACE(log, "loadIds " + toString() + " ids=" + std::to_string(ids.size()));

	ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & out_stream) {

		WriteBufferFromOStream out_buffer(out_stream);
		idsToBuffer(context, format, sample_block, out_buffer, ids);
	};

	Poco::URI uri(url);
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	LOG_TRACE(log, "loadKeys " + toString() + " rows=" + std::to_string(requested_rows.size()));

	ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & out_stream) {
		WriteBufferFromOStream out_buffer(out_stream);
		columnsToBuffer(context, format, sample_block, out_buffer, dict_struct, key_columns, requested_rows);
	};

	Poco::URI uri(url);
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));
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
	Poco::URI uri(url);
	return uri.toString();
}

}
