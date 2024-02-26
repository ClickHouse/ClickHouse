#include "NullDictionarySource.h"
#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"


namespace DB
{
NullDictionarySource::NullDictionarySource(Block & sample_block_) : sample_block(sample_block_)
{
}

NullDictionarySource::NullDictionarySource(const NullDictionarySource & other) : sample_block(other.sample_block)
{
}

QueryPipeline NullDictionarySource::loadAll()
{
    LOG_TRACE(&Poco::Logger::get("NullDictionarySource"), "loadAll {}", toString());
    return QueryPipeline(std::make_shared<NullSource>(sample_block));
}


std::string NullDictionarySource::toString() const
{
    return "Null";
}


void registerDictionarySourceNull(DictionarySourceFactory & factory)
{
    auto create_table_source
        = [=](const DictionaryStructure & /* dict_struct */,
              const Poco::Util::AbstractConfiguration & /* config */,
              const std::string & /* config_prefix */,
              Block & sample_block,
              ContextPtr /* global_context */,
              const std::string & /* default_database */,
              bool /* created_from_ddl*/) -> DictionarySourcePtr { return std::make_unique<NullDictionarySource>(sample_block); };

    factory.registerSource("null", create_table_source);
}

}
