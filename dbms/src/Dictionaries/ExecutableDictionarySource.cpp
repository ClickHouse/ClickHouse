#include <DB/Dictionaries/ExecutableDictionarySource.h>

#include <DB/Common/ShellCommand.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBufferBlockInputStream.h>
#include <DB/IO/ReadBufferFromString.h>

namespace DB
{


decltype(ExecutableDictionarySource::max_block_size) ExecutableDictionarySource::max_block_size;

ExecutableDictionarySource::ExecutableDictionarySource(const std::string & name, const std::string & format, Block & sample_block, const Context & context)
	: name{name}, format{format}, sample_block{sample_block}, context(context)
{
		last_modification = LocalDateTime {std::time(nullptr)};
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
	: name{other.name},
	  format{other.format},
	  sample_block{other.sample_block}, context(other.context),
	  last_modification{other.last_modification}
{
}


BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
	last_modification = getLastModification();
	//LOG_TRACE(log, );

	std::string exec_result;

	{
		auto process = ShellCommand::execute(name);
		readStringUntilEOF(exec_result, process->out);
		process->wait();
	}

std::cerr << "readed [" <<  exec_result  << "] format=" << format << std::endl;

	auto in_ptr = std::make_unique<ReadBufferFromString>(exec_result);
	auto stream = context.getInputFormat( format, *in_ptr, sample_block, max_block_size);
	return std::make_shared<OwningBufferBlockInputStream>(stream, std::move(in_ptr));
}

BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

bool ExecutableDictionarySource::isModified() const
{
	return getLastModification() > last_modification;
}

bool ExecutableDictionarySource::supportsSelectiveLoad() const
{
	return false;
}

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
	return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
	return "Executable: " + name;
}

LocalDateTime ExecutableDictionarySource::getLastModification() const
{
	return last_modification;
}


}
