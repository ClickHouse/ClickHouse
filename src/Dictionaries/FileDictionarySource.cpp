#include "FileDictionarySource.h"

#include <Poco/File.h>
#include <filesystem>

#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"
#include "DictionarySourceHelpers.h"

namespace DB
{
static const UInt64 max_block_size = 8192;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


FileDictionarySource::FileDictionarySource(
    const std::string & filepath_, const std::string & format_,
    Block & sample_block_, ContextPtr context_, bool check_config)
    : filepath{filepath_}
    , format{format_}
    , sample_block{sample_block_}
    , context(context_)
{
    if (check_config)
    {
        auto source_file_path = std::filesystem::path(filepath);
        auto source_file_absolute_path = std::filesystem::canonical(source_file_path);

        String user_files_path_string_value = context->getUserFilesPath();
        auto user_files_path = std::filesystem::path(user_files_path_string_value);
        auto user_files_absolute_path = std::filesystem::canonical(user_files_path);

        auto [_, user_files_absolute_path_mismatch_it] = std::mismatch(source_file_absolute_path.begin(), source_file_absolute_path.end(), user_files_absolute_path.begin(), user_files_absolute_path.end());

        bool user_files_absolute_path_include_source_file_absolute_path = user_files_absolute_path_mismatch_it == user_files_absolute_path.end();

        if (!user_files_absolute_path_include_source_file_absolute_path)
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", filepath, user_files_path_string_value);
    }
}


FileDictionarySource::FileDictionarySource(const FileDictionarySource & other)
    : filepath{other.filepath}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , last_modification{other.last_modification}
{
}


BlockInputStreamPtr FileDictionarySource::loadAll()
{
    LOG_TRACE(&Poco::Logger::get("FileDictionary"), "loadAll {}", toString());
    auto in_ptr = std::make_unique<ReadBufferFromFile>(filepath);
    auto stream = context->getInputFormat(format, *in_ptr, sample_block, max_block_size);
    last_modification = getLastModification();

    return std::make_shared<OwningBlockInputStream<ReadBuffer>>(stream, std::move(in_ptr));
}


std::string FileDictionarySource::toString() const
{
    return fmt::format("File: {}, {}", filepath, format);
}


Poco::Timestamp FileDictionarySource::getLastModification() const
{
    return Poco::File{filepath}.getLastModified();
}

void registerDictionarySourceFile(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `file` does not support attribute expressions");

        const auto filepath = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");

        auto context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        return std::make_unique<FileDictionarySource>(filepath, format, sample_block, context_local_copy, check_config);
    };

    factory.registerSource("file", create_table_source);
}

}
