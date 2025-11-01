#include <filesystem>
#include <Formats/FormatSchemaInfo.h>

#include <fcntl.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Disks/IO/WriteBufferFromTemporaryFile.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/QueryFlags.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <boost/algorithm/hex.hpp>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/SipHash.h>
#include <Common/atomicRename.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace
{
    String getFormatSchemaDefaultFileExtension(const String & format)
    {
        if (format == "Protobuf")
            return "proto";
        if (format == "CapnProto")
            return "capnp";
        return "";
    }
}


FormatSchemaInfo::FormatSchemaInfo(
    const String & format_schema_source,
    const String & format_schema,
    const String & format_schema_message_name,
    const String & format,
    bool require_message,
    bool is_server,
    const String & format_schema_path,
    bool generated_)
    : message_name(format_schema_message_name)
    , generated(generated_)
    , log(getLogger("FormatSchemaInfo"))
{
    if (format_schema_source.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The format_schema_source setting should be set");

    if (format_schema.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The format {} requires a schema. The corresponding setting should be set", format);

    if (!require_message && !message_name.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "format_schema_message_name is specified {} when not required", message_name);
    }

    auto default_schema_directory = [&format_schema_path]()
    {
        static const String str = fs::canonical(format_schema_path) / "";
        return str;
    };

    if (format_schema_source == FormatSettings::FORMAT_SCHEMA_SOURCE_FILE)
    {
        handleSchemaFile(format_schema, format, require_message, is_server, default_schema_directory());
        return;
    }

    if (format_schema_source == FormatSettings::FORMAT_SCHEMA_SOURCE_STRING)
    {
        handleSchemaContent(format_schema, format, is_server, default_schema_directory());
        return;
    }

    if (format_schema_source == FormatSettings::FORMAT_SCHEMA_SOURCE_QUERY)
    {
        handleSchemaSourceQuery(format_schema, format, is_server, default_schema_directory());
        return;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid format_schema_source setting. Possible value: '{}', '{}', '{}', actual '{}'",
        FormatSettings::FORMAT_SCHEMA_SOURCE_FILE,
        FormatSettings::FORMAT_SCHEMA_SOURCE_STRING,
        FormatSettings::FORMAT_SCHEMA_SOURCE_QUERY,
        format_schema_source);
}

FormatSchemaInfo::FormatSchemaInfo(const FormatSettings & settings, const String & format, bool require_message)
    : FormatSchemaInfo(
          settings.schema.format_schema_source,
          settings.schema.format_schema,
          settings.schema.format_schema_message_name,
          format,
          require_message,
          settings.schema.is_server,
          settings.schema.format_schema_path)
{
}

void FormatSchemaInfo::handleSchemaFile(
    const String & format_schema, const String & format, bool require_message, bool is_server, const String & format_schema_path)
{
    String default_file_extension = getFormatSchemaDefaultFileExtension(format);

    fs::path path;
    verifySchemaFileName(format_schema, require_message, path);
    processSchemaFile(path, default_file_extension, is_server, format_schema_path);
}

void FormatSchemaInfo::verifySchemaFileName(const String & format_schema, bool require_message, fs::path & path)
{
    size_t colon_pos = format_schema.find(':');
    String format_schema_file_name;
    String format_schema_message_name;
    if (colon_pos != String::npos)
    {
        if (!message_name.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Require message is already defined in `format_schema_message_name` and should not be defined in 'format_schema'");

        format_schema_file_name = format_schema.substr(0, colon_pos);
        format_schema_message_name = format_schema.substr(colon_pos + 1);
    }
    else
    {
        format_schema_file_name = format_schema;
    }

    if (require_message)
    {
        if (message_name.empty() && format_schema_message_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected 'format_schema_message_name' setting");

        if (message_name.empty())
            message_name = format_schema_message_name;

        path = fs::path(format_schema_file_name);
        String filename = path.has_filename() ? path.filename() : path.parent_path().filename();
        if (filename.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid file name in 'format_schema' setting");
    }
    else
    {
        path = fs::path(format_schema_file_name);
        if (!path.has_filename())
            path = path.parent_path() / "";
    }
}

void FormatSchemaInfo::handleSchemaContent(const String & content, const String & format, bool is_server, const String & format_schema_path)
{
    String default_file_extension = getFormatSchemaDefaultFileExtension(format);
    auto file_name = generateSchemaFileName(content, default_file_extension);
    auto cached_file_path = fs::path(CACHE_DIR_NAME) / file_name;
    auto file_path = fs::path(format_schema_path) / cached_file_path;
    if (fs::exists(file_path))
    {
        LOG_DEBUG(log, "Cached file exists '{}', skip storing schema file", file_path.string());
    }
    else
    {
        storeSchemaOnDisk(/*file_path=*/file_path, /*content=*/content);
    }
    processSchemaFile(cached_file_path, default_file_extension, is_server, format_schema_path);
}

void FormatSchemaInfo::handleSchemaSourceQuery(
    const String & format_schema, const String & format, bool is_server, const String & format_schema_path)
{
    String default_file_extension = getFormatSchemaDefaultFileExtension(format);
    auto file_name = generateSchemaFileName(format_schema, default_file_extension);
    auto cached_file_path = fs::path(CACHE_DIR_NAME) / file_name;
    auto file_path = fs::path(format_schema_path) / cached_file_path;
    if (fs::exists(file_path))
    {
        LOG_DEBUG(log, "Cached file exists '{}' for query '{}', skip querying schema", file_path.string(), format_schema);
    }
    else
    {
        auto content = querySchema(format_schema);
        storeSchemaOnDisk(/*file_path=*/file_path, /*content=*/content);
    }
    processSchemaFile(cached_file_path, default_file_extension, is_server, format_schema_path);
}

String FormatSchemaInfo::querySchema(const String & query)
{
    auto current_query_context = CurrentThread::get().getQueryContext();
    if (!current_query_context)
        current_query_context = Context::getGlobalContextInstance();

    ParserSelectQuery parser;
    ASTPtr select_ast = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    auto query_context = Context::createCopy(current_query_context);
    InterpreterSelectQuery interpreter(select_ast, query_context, SelectQueryOptions().setInternal());
    BlockIO io = interpreter.execute();

    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    std::optional<String> result;
    while (executor.pull(block))
    {
        if (result.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected the schema query result to have one row");
        if (block.columns() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected the schema query result to have one column");

        auto & column = block.getByPosition(0).column;
        if (const auto * col_str = typeid_cast<const ColumnString *>(column.get()))
        {
            result = col_str->getDataAt(0).toString();
            continue;
        }

        if (const auto * col_const = typeid_cast<const ColumnConst *>(column.get()))
        {
            if (const auto * col_str_const = typeid_cast<const ColumnString *>(col_const->getDataColumnPtr().get()))
            {
                result = col_str_const->getDataAt(0).toString();
                continue;
            }
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected the schema query result to have one String column");
    }
    if (!result.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema query result is empty");

    return *result;
}

void FormatSchemaInfo::storeSchemaOnDisk(const fs::path & file_path, const String & content)
{
    if (!file_path.has_filename() || file_path.filename().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid file path {}", file_path.string());

    if (fs::exists(file_path))
    {
        LOG_DEBUG(log, "Cache file {} existed, skip storing", file_path.string());
        return;
    }

    LOG_INFO(log, "Storing schema to {}", file_path.string());
    auto dir_path = file_path.parent_path();
    if (!fs::exists(dir_path))
    {
        fs::create_directory(dir_path);
    }

    auto temp_path = fs::path(file_path.string() + ".tmp");

    try
    {
        WriteBufferFromFile out(temp_path, content.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(content, out);
        out.next();
        out.sync();
        out.close();

        if (fs::exists(file_path))
            DB::renameExchange(temp_path, file_path);
        else
            DB::renameNoReplace(temp_path, file_path);

        fs::remove(temp_path);
    }
    catch (...)
    {
        fs::remove(temp_path);
        tryLogCurrentException("FormatSchemaInfo", "Unable to store schema file " + file_path.string() + " on disk");
        if (!fs::exists(file_path))
            throw;
    }
}

void FormatSchemaInfo::processSchemaFile(
    fs::path path, const String & default_file_extension, bool is_server, const String & format_schema_path)
{
    if (!path.has_extension() && !default_file_extension.empty())
        path = path.parent_path() / (path.stem().string() + '.' + default_file_extension);

    fs::path default_schema_directory_path(format_schema_path);
    if (path.is_absolute())
    {
        if (is_server)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Absolute path in the 'format_schema' setting is prohibited: {}", path.string());
        schema_path = path.filename();
        schema_directory = path.parent_path() / "";
    }
    else if (
        path.has_parent_path()
        && !fs::weakly_canonical(default_schema_directory_path / path)
                .string()
                .starts_with(fs::weakly_canonical(default_schema_directory_path).string()))
    {
        if (is_server)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path in the 'format_schema' setting shouldn't go outside the 'format_schema_path' directory: {} ({} not in {})",
                format_schema_path,
                path.string(),
                format_schema_path);
        path = default_schema_directory_path / path;
        schema_path = path.filename();
        schema_directory = path.parent_path() / "";
    }
    else
    {
        schema_path = path;
        schema_directory = format_schema_path;
    }
}

String FormatSchemaInfo::generateSchemaFileName(const String & hashing_content, const String & file_extention)
{
#if USE_SSL
    String content_hash_hex;
    auto hash = encodeSHA256(hashing_content);
    content_hash_hex.resize(hash.size() * 2);
    boost::algorithm::hex(hash.begin(), hash.end(), content_hash_hex.data());
#else
    String content_hash_hex = getHexUIntLowercase(sipHash64(hashing_content));
#endif

    static constexpr size_t CONTENT_SAMPLE_MAX_LEN = 32;
    size_t content_sample_len = std::min(CONTENT_SAMPLE_MAX_LEN, hashing_content.size());

    String content_sample;
    content_sample.resize(content_sample_len * 2);
    boost::algorithm::hex(content_sample.begin(), content_sample.begin() + content_sample_len, content_sample.data());

    if (file_extention.empty())
        return fmt::format("{}-{}", content_sample, content_hash_hex);
    else
        return fmt::format("{}-{}.{}", content_sample, content_hash_hex, file_extention);
}

template <typename SchemaGenerator>
MaybeAutogeneratedFormatSchemaInfo<SchemaGenerator>::MaybeAutogeneratedFormatSchemaInfo(
    const FormatSettings & settings, const String & format, const Block & header, bool use_autogenerated_schema, bool with_envelope)
{
    if (!use_autogenerated_schema || !settings.schema.format_schema.empty())
    {
        schema_info = std::make_unique<FormatSchemaInfo>(settings, format, true);
        return;
    }

    String schema_path;
    fs::path default_schema_directory_path(fs::canonical(settings.schema.format_schema_path) / "");
    fs::path path;
    if (!settings.schema.output_format_schema.empty())
    {
        schema_path = settings.schema.output_format_schema;
        path = schema_path;
        if (path.is_absolute())
        {
            if (settings.schema.is_server)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Absolute path in the 'output_format_schema' setting is prohibited: {}", path.string());
        }
        else if (path.has_parent_path() && !fs::weakly_canonical(default_schema_directory_path / path).string().starts_with(fs::weakly_canonical(default_schema_directory_path).string()))
        {
            if (settings.schema.is_server)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Path in the 'format_schema' setting shouldn't go outside the 'format_schema_path' directory: {} ({} not in {})",
                    default_schema_directory_path.string(),
                    path.string(),
                    default_schema_directory_path.string());
            path = default_schema_directory_path / path;
        }
        else
        {
            path = default_schema_directory_path / path;
        }
    }
    else
    {
        if (settings.schema.is_server)
        {
            tmp_file_path = Poco::TemporaryFile::tempName(default_schema_directory_path.string()) + '.' + getFormatSchemaDefaultFileExtension(format);
            schema_path = fs::path(tmp_file_path).filename();
        }
        else
        {
            tmp_file_path = Poco::TemporaryFile::tempName() + '.' + getFormatSchemaDefaultFileExtension(format);
            schema_path = tmp_file_path;
        }

        path = tmp_file_path;
    }

    WriteBufferFromFile buf(path.string());
    SchemaGenerator::writeSchema(buf, "Message", header.getNamesAndTypesList(), with_envelope);
    buf.finalize();

    schema_info = std::make_unique<FormatSchemaInfo>(
        /*format_schema_source=*/FormatSettings::FORMAT_SCHEMA_SOURCE_FILE,
        /*format_schema=*/schema_path,
        /*format_schema_message_name=*/"Message",
        /*format=*/format,
        /*require_message=*/true,
        /*is_server*/ settings.schema.is_server,
        /*format_schema_path=*/settings.schema.format_schema_path,
        /*generated*/ true);
}

template <typename SchemaGenerator>
MaybeAutogeneratedFormatSchemaInfo<SchemaGenerator>::~MaybeAutogeneratedFormatSchemaInfo()
{
    if (!tmp_file_path.empty())
    {
        try
        {
            fs::remove(tmp_file_path);
        }
        catch (...)
        {
            tryLogCurrentException("MaybeAutogeneratedFormatSchemaInfo", "Cannot delete temporary schema file");
        }
    }
}

template class MaybeAutogeneratedFormatSchemaInfo<StructureToCapnProtoSchema>;
template class MaybeAutogeneratedFormatSchemaInfo<StructureToProtobufSchema>;

}
