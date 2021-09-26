#include "YAMLDictionarySource.h"
#include <Formats/FormatFactory.h>

#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"

namespace DB
{
static const UInt64 max_block_size = 8192;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


YAMLDictionarySource::YAMLDictionarySource(
            const std::string & filepath_, 
            Block & sample_block_,
            ContextPtr context_,
            bool created_from_ddl)
            : filepath{filepath_}
            , sample_block{sample_block_}
            , context(context_)
{
    auto user_files_path = context->getUserFilesPath();
    if (created_from_ddl && !pathStartsWith(filepath, user_files_path))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", filepath, user_files_path);
}


YAMLDictionarySource::YAMLDictionarySource(const YAMLDictionarySource & other)
    : filepath{other.filepath}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , last_modification{other.last_modification}
{
}

Pipe YAMLDictionarySource::loadAll()
{
    LOG_TRACE(&Poco::Logger::get("YAMLDictionarySource"), "loadAll {}", toString());
    auto buf = std::make_unique<ReadBufferFromFile>(filepath);
    auto source = FormatFactory::instance().getInput("YAML", *buf, sample_block, context, max_block_size);
    source->addBuffer(std::move(buf));
    last_modification = getLastModification();

    return Pipe(std::move(source));
}

Poco::Timestamp YAMLDictionarySource::getLastModification() const
{
    return FS::getModificationTimestamp(filepath);
}

std::string YAMLDictionarySource::toString() const
{
    return fmt::format("YAML file: {}", filepath);
}

void registerDictionarySourceYAML(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type 'yaml' does not support attribute expressions");

        const auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        const auto filepath = config.getString(config_prefix + ".yaml.path");

        return std::make_unique<YAMLDictionarySource>(filepath, sample_block, context, created_from_ddl);
    };

    factory.registerSource("yaml", create_table_source);
}

}

