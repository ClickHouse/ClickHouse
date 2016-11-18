#include <DB/Dictionaries/ExecutableDictionarySource.h>

#include <DB/Common/ShellCommand.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>

//#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

ExecutableDictionarySource::ExecutableDictionarySource(const DictionaryStructure & dict_struct_, const std::string & name, const std::string & format, Block & sample_block, const Context & context) :
	dict_struct{dict_struct_},
	name{name},
	format{format},
	sample_block{sample_block},
	context(context)
{
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other) :
	  dict_struct{other.dict_struct},
	  name{other.name},
	  format{other.format},
	  sample_block{other.sample_block},
	  context(other.context)
{
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
	//std::cerr << "ExecutableDictionarySource::loadAll " <<std::endl;
	LOG_TRACE(log, "execute " + name);
	auto process = ShellCommand::execute(name);
	auto stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	//std::cerr << "ExecutableDictionarySource::loadIds s=" << ids.size() <<std::endl;
	auto process = ShellCommand::execute(name);

	{
		ColumnWithTypeAndName column;
		column.type = std::make_shared<DataTypeUInt64>();
		column.column = column.type->createColumn();

		for (auto & id : ids) {
			column.column->insert(id); //maybe faster?
		}

		Block block;
		block.insert(std::move(column));

		auto stream_out = context.getOutputFormat(format, process->in, sample_block);
		stream_out->write(block);
	}

/*
	for (auto & id : ids) {
		writeString(std::to_string(id), process->in);
		writeString("\n", process->in);
	}
*/

	process->in.close();

/*
	std::string process_err;
	readStringUntilEOF(process_err, process->err);
	std::cerr << "readed ERR [" <<  process_err  << "] " << std::endl;
*/

	auto stream = context.getInputFormat( format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	//std::cerr << " ExecutableDictionarySource::loadKeys cols=" << key_columns.size() << " rows=" <<requested_rows.size()   << std::endl;
	auto process = ShellCommand::execute(name);

	{
		Block block;
		for(auto & key : key_columns) {
			ColumnWithTypeAndName column;
			column.type = std::make_shared<DataTypeUInt64>(); // TODO TYPE
			//column.column = column.type->createColumn();
			//column.column.reset(const_cast<DB::IColumn*>(key));
			column.column = key->clone(); // wrong!
			//column.column = key->convertToFullColumnIfConst(); // check!
			block.insert(std::move(column));
		}

		auto stream_out = context.getOutputFormat(format, process->in, sample_block);
		stream_out->write(block);

	}
/*
	for (const auto row : requested_rows)
	{
		writeString(std::to_string(row), process->in);
		writeString("\n", process->in); // TODO: format?
	}
*/

		process->in.close();

/*
	std::string process_err;
	readStringUntilEOF(process_err, process->err);
	std::cerr << "readed ERR [" <<  process_err  << "] " << std::endl;
*/

	auto stream = context.getInputFormat( format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

bool ExecutableDictionarySource::isModified() const
{
	return true;
}

bool ExecutableDictionarySource::supportsSelectiveLoad() const
{
	return true;
}

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
	return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
	return "Executable: " + name;
}

}
