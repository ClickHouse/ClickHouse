#include <Dictionaries/NullDictionarySource.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Common/logger_useful.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/registerDictionaries.h>


namespace DB
{
NullDictionarySource::NullDictionarySource(SharedHeader sample_block_) : sample_block(sample_block_)
{
}

NullDictionarySource::NullDictionarySource(const NullDictionarySource & other) : sample_block(other.sample_block)
{
}

BlockIO NullDictionarySource::loadAll()
{
    LOG_TRACE(getLogger("NullDictionarySource"), "loadAll {}", toString());
    BlockIO io;
    io.pipeline = QueryPipeline(std::make_shared<NullSource>(sample_block));
    return io;
}


std::string NullDictionarySource::toString() const
{
    return "Null";
}


void registerDictionarySourceNull(DictionarySourceFactory & factory)
{
    auto create_table_source
        = [=](const String & /*name*/,
              const DictionaryStructure & /* dict_struct */,
              const Poco::Util::AbstractConfiguration & /* config */,
              const std::string & /* config_prefix */,
              Block & sample_block,
              ContextPtr /* global_context */,
              const std::string & /* default_database */,
              bool /* created_from_ddl*/) -> DictionarySourcePtr { return std::make_unique<NullDictionarySource>(std::make_shared<const Block>(sample_block)); };

    factory.registerSource("null", create_table_source);
}

}
