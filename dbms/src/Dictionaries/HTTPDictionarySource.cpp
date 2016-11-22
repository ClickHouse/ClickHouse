#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>
#include <DB/IO/ReadBufferFromHTTP.h>
#include <DB/IO/ReadWriteBufferFromHTTP.h>

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/IO/WriteBufferFromOStream.h>

namespace DB
{

HTTPDictionarySource::HTTPDictionarySource(const DictionaryStructure & dict_struct_,
		const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	host{config.getString(config_prefix + ".host", "::1")},
	port{std::stoi(config.getString(config_prefix + ".port", "80"))},
	path{config.getString(config_prefix + ".path", "")},
	//method{config.getString(config_prefix + ".method", "")},
	format{config.getString(config_prefix + ".format")},
	selective{!config.getString(config_prefix + ".selective", "").empty()}, // todo! how to correct?
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
	selective{other.selective},
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

	HTTPLocation http_location;
	http_location.host = host;
	http_location.port = port;
	http_location.path = path;
	http_location.method = Poco::Net::HTTPRequest::HTTP_POST;
	ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & out_stream) {
		// copypaste from ExecutableDictionarySource.cpp, todo: make func
		ColumnWithTypeAndName column;
		column.type = std::make_shared<DataTypeUInt64>();
		column.column = column.type->createColumn();

		for (auto & id : ids) {
			column.column->insert(id); //CHECKME maybe faster?
		}

		Block block;
		block.insert(std::move(column));

		WriteBufferFromOStream out_buffer(out_stream);
		auto stream_out = context.getOutputFormat(format, out_buffer, sample_block);
		stream_out->write(block);
	};
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(http_location, out_stream_callback);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));

}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	LOG_TRACE(log, "loadKeys " + toString());

	HTTPLocation http_location;
	http_location.host = host;
	http_location.port = port;
	http_location.path = path;
	http_location.method = Poco::Net::HTTPRequest::HTTP_POST;
	ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & out_stream) {
		// copypaste from ExecutableDictionarySource.cpp, todo: make func
		Block block;

		const auto keys_size = key_columns.size();
		for (const auto i : ext::range(0, keys_size))
		{

			const auto & key_description = (*dict_struct.key)[i];
			const auto & key = key_columns[i];

			ColumnWithTypeAndName column;
			column.type = key_description.type;
			column.column = key->clone(); // CHECKME !!
			block.insert(std::move(column));
		}

		WriteBufferFromOStream out_buffer(out_stream);
		auto stream_out = context.getOutputFormat(format, out_buffer, sample_block);
		stream_out->write(block);
	};

	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(http_location, out_stream_callback);
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));

}

bool HTTPDictionarySource::isModified() const
{
	return true;
}

bool HTTPDictionarySource::supportsSelectiveLoad() const
{
	return selective;
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
