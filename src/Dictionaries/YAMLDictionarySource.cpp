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
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


YAMLDictionarySource::YAMLDictionarySource(
            const std::string & filepath_, 
            //const Configuration & configuration,
            Block & sample_block_,
            ContextPtr context_,
            bool created_from_ddl)
            : filepath{filepath_}
            //, configuration(configuration_)
            , sample_block{sample_block_}
            , context(context_)
{
    if (created_from_ddl && !pathStartsWith(filepath, context->getUserFilesPath()))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", filepath, context->getUserFilesPath());
}


YAMLDictionarySource::YAMLDictionarySource(const YAMLDictionarySource & other)
    : filepath{other.filepath}
    //, configuration(other.configuration)
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , last_modification{other.last_modification}
{
}

InputFormatPtr YAMLDictionarySource::getYAMLInputFormat(
    ReadBuffer & buf,
    const Block & sample,
    ContextPtr context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & input_getter = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        UInt64 max_block_size,
        ReadCallback callback,
        const FormatSettings & settings)>;
    const Settings & settings = context->getSettingsRef();
    
    //if (context->hasQueryContext() && settings.log_queries)
    //    context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);  //format?

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    
    RowInputFormatParams params;
    params.max_block_size = 8192;
    params.allow_errors_num = 0;
    params.allow_errors_ratio = 0;
    params.max_execution_time = settings.max_execution_time;
    params.timeout_overflow_mode = settings.timeout_overflow_mode;
    
    auto format = input_getter(buf, sample, params, format_settings);//?
    
    /// It's a kludge. Because I cannot remove context from values format.
    if (auto * values = typeid_cast<ValuesBlockInputFormat *>(format.get()))
        values->setContext(context);
        
    return format;
}

InputFormatPtr YAMLDictionarySource::getYAMLInput(
    ReadBuffer & buf,
    const Block & sample,
    ContextPtr context,
    const std::optional<FormatSettings> & _format_settings) const
{
    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    auto format = getYAMLInputFormat(buf, sample, context);
    return format;
}

int skipWhitespaceIfAnyAndGetDiff(int spaces, ReadBuffer & buf)
{
    int count = 0;
    while (!buf.eof() && isWhitespaceASCIIOneLine(*buf.position()))
    {
        ++buf.position();
        ++count;
    }
    return count - spaces;
}

std::pair<String, String> parseString(String & s, ReadBuffer & buf)
{
    //skipWhitespaceIfAny(buf);
    readStringUntilWhitespace(s, buf);
    String key = s;
    String value;
    if (s != "set:")
    {
        skipWhitespaceIfAny(buf);
        readEscapedStringUntilEOL(s, buf);
        value = s;
    }
    else
    {
        value = "";
    }
    skipToNextLineOrEOF(buf);
    return std::pair<String, String>(key, value);
}

Pipe YAMLDictionarySource::loadAll()
{
    LOG_TRACE(&Poco::Logger::get("YAMLDictionarySource"), "loadAll {}", toString());
    auto buf = std::make_unique<ReadBufferFromFile>(filepath);
    String s;
    int spaces = 0;
    std::map<int, int> parents;
    parents[0] = -1;
    int current_id = 0;
    std::map<String, String> data;
    String name;
    bool wasSet = false;
    while(!buf->eof())
    {
        int diff = skipWhitespaceIfAnyAndGetDiff(spaces, *buf);
        spaces+=diff;
        std::pair<String, String> keyAndValue = parseString(s, *buf);
        if(spaces == 0 && diff == 0)
        {
            if(keyAndValue.first == "match:")
            {
                std::map<String, String> data;
                name = keyAndValue.second;
            }
            else if(keyAndValue.first == "set:")
            {
                wasSet = true;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected match: or set:, your string is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
            }
        }
        else if(diff == 1)
        {
            if(keyAndValue.first == "match:")
            {
                //SAVEDATA(current_id, parents[spaces-1], name, data);
                parents[spaces] = current_id;
                std::map<String, String> data;
                name = keyAndValue.second;
                ++current_id;
            }
            else if(keyAndValue.first == "set:")
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indentation problem if diff = 1: key is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
            }
            else
            {
                data[keyAndValue.first] = keyAndValue.second;
                parents[spaces] = parents[spaces-1];
                wasSet = false;
            }
        }
        else if(diff == 0)
        {
            if(keyAndValue.first == "set:")
            {
                wasSet = true;
            }
            else if(keyAndValue.first == "match:")
            {
                if(wasSet)
                {
                    //SAVEDATA(current_id, parents[space], name, data);
                    std::map<String, String> data;
                    name = keyAndValue.second;
                    ++current_id;
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Indentation problem if diff = 0: key is {}, value is {}, spaces are {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
                }
            }
            else
            {
                data[keyAndValue.first] = keyAndValue.second;
                wasSet = false;
            }
        }
        else if(diff < 0)
        {
            if(keyAndValue.first == "match:")
            {
                //SAVEDATA(current_id, parents[spaces+diff], name, data);
                std::map<String, String> data;
                name = keyAndValue.second;
                ++current_id;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected match:, your string is {}, value is {}, spaces is {}, diff is {}", keyAndValue.first, keyAndValue.second, spaces, diff);
            }
        }
        if(buf->eof())
        {
            //SAVEDATA(current_id, parents[spaces], name, data);
            break;
        }
    }
    auto source = getYAMLInput(*buf, sample_block, context);
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

        //YAMLDictionarySource::Configuration configuration
        //{

        //};

        return std::make_unique<YAMLDictionarySource>(filepath, sample_block, context, created_from_ddl);
    };

    factory.registerSource("yaml", create_table_source);
}

}

