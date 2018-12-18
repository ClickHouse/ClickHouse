#include "FileDictionarySource.h"

#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{
static const size_t max_block_size = 8192;


FileDictionarySource::FileDictionarySource(
    const std::string & filename, const std::string & format, Block & sample_block, const Context & context)
    : filename{filename}, format{format}, sample_block{sample_block}, context(context)
{
}


FileDictionarySource::FileDictionarySource(const FileDictionarySource & other)
    : filename{other.filename}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(other.context)
    , last_modification{other.last_modification}
{
}


BlockInputStreamPtr FileDictionarySource::loadAll()
{
    auto in_ptr = std::make_unique<ReadBufferFromFile>(filename);
    auto stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
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

void registerDictionarySourceFile(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context) -> DictionarySourcePtr {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `file` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        const auto filename = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");

        return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
    };

    factory.registerSource("file", createTableSource);
}

}
