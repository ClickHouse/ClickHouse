#include <DB/Dictionaries/ExecutableDictionarySource.h>

#include <DB/Common/ShellCommand.h>
#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/OwningBlockInputStream.h>

namespace DB
{

ExecutableDictionarySource::ExecutableDictionarySource(const std::string & name, const std::string & format, Block & sample_block, const Context & context)
	: name{name}, format{format}, sample_block{sample_block}, context(context)
{
		last_modification = std::time(nullptr);
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
	LOG_TRACE(log, "execute " + name);
	auto process = ShellCommand::execute(name);
	auto stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
	return std::make_shared<OwningBlockInputStream<ShellCommand>>(stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	auto process = ShellCommand::execute(name);

	for (auto & id : ids) {
		writeString(std::to_string(id), process->in);
		writeString("\n", process->in); // TODO: format?
	}

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
std::cerr << "ExecutableDictionarySource::loadKeys " << requested_rows.size()  << std::endl;
	throw Exception{"Method unsupported", ErrorCodes::NOT_IMPLEMENTED};
}

bool ExecutableDictionarySource::isModified() const
{
	return getLastModification() > last_modification;
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

LocalDateTime ExecutableDictionarySource::getLastModification() const
{
	return last_modification;
}


}
