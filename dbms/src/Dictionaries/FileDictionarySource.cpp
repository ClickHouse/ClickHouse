#include <Interpreters/Context.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Dictionaries/FileDictionarySource.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>


namespace DB
{

static const size_t max_block_size = 8192;


FileDictionarySource::FileDictionarySource(const std::string & filename, const std::string & format, Block & sample_block,
    const Context & context)
    : filename{filename}, format{format}, sample_block{sample_block}, context(context)
{}


FileDictionarySource::FileDictionarySource(const FileDictionarySource & other)
    : filename{other.filename}, format{other.format},
        sample_block{other.sample_block}, context(other.context),
        last_modification{other.last_modification}
{}


BlockInputStreamPtr FileDictionarySource::loadAll()
{
    auto in_ptr = std::make_unique<ReadBufferFromFile>(filename);
    auto stream = context.getInputFormat(
        format, *in_ptr, sample_block, max_block_size);
    last_modification = getLastModification();

    return std::make_shared<OwningBlockInputStream<ReadBuffer>>(stream, std::move(in_ptr));
}


std::string FileDictionarySource::toString() const
{
    return "File: " + filename + ' ' + format;
}


Poco::Timestamp FileDictionarySource::getLastModification() const
{
    return Poco::File{filename}.getLastModified();
}

}
