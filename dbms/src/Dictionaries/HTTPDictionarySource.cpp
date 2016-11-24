#include <Poco/Net/HTTPRequest.h>

#include <DB/Dictionaries/HTTPDictionarySource.h>

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>
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
	LOG_TRACE(log, "loadIds " + toString());

	std::ostringstream out_stream;
	{
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

	Poco::URI uri(url);
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream.str());
	auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	LOG_TRACE(log, "loadKeys " + toString());

	std::ostringstream out_stream;
	{
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

	Poco::URI uri(url);
	auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream.str());
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
