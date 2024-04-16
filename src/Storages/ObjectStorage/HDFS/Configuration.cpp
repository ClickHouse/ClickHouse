#include <Storages/ObjectStorage/HDFS/Configuration.h>

#if USE_HDFS
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/IAST.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageHDFSConfiguration::StorageHDFSConfiguration(const StorageHDFSConfiguration & other)
    : StorageObjectStorageConfiguration(other)
{
    url = other.url;
    path = other.path;
    paths = other.paths;
}

void StorageHDFSConfiguration::check(ContextPtr context) const
{
    context->getRemoteHostFilter().checkURL(Poco::URI(url));
    checkHDFSURL(fs::path(url) / path.substr(1));
}

ObjectStoragePtr StorageHDFSConfiguration::createObjectStorage( /// NOLINT
    ContextPtr context,
    bool /* is_readonly */)
{
    assertInitialized();
    const auto & settings = context->getSettingsRef();
    auto hdfs_settings = std::make_unique<HDFSObjectStorageSettings>(
        settings.remote_read_min_bytes_for_seek,
        settings.hdfs_replication
    );
    return std::make_shared<HDFSObjectStorage>(url, std::move(hdfs_settings), context->getConfigRef());
}

std::string StorageHDFSConfiguration::getPathWithoutGlob() const
{
    /// Unlike s3 and azure, which are object storages,
    /// hdfs is a filesystem, so it cannot list files by partual prefix,
    /// only by directory.
    auto first_glob_pos = path.find_first_of("*?{");
    auto end_of_path_without_globs = path.substr(0, first_glob_pos).rfind('/');
    if (end_of_path_without_globs == std::string::npos || end_of_path_without_globs == 0)
        return "/";
    return path.substr(0, end_of_path_without_globs);
}

void StorageHDFSConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    const size_t max_args_num = with_structure ? 4 : 3;
    if (args.empty() || args.size() > max_args_num)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Expected not more than {} arguments", max_args_num);
    }

    std::string url_str;
    url_str = checkAndGetLiteralArgument<String>(args[0], "url");

    if (args.size() > 1)
    {
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
        format = checkAndGetLiteralArgument<String>(args[1], "format_name");
    }

    if (with_structure)
    {
        if (args.size() > 2)
        {
            args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
            structure = checkAndGetLiteralArgument<String>(args[2], "structure");
        }
        if (args.size() > 3)
        {
            args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);
            compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
        }
    }
    else if (args.size() > 2)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        compression_method = checkAndGetLiteralArgument<String>(args[2], "compression_method");
    }

    setURL(url_str);
}

void StorageHDFSConfiguration::fromNamedCollection(const NamedCollection & collection)
{
    std::string url_str;

    auto filename = collection.getOrDefault<String>("filename", "");
    if (!filename.empty())
        url_str = std::filesystem::path(collection.get<String>("url")) / filename;
    else
        url_str = collection.get<String>("url");

    format = collection.getOrDefault<String>("format", "auto");
    compression_method = collection.getOrDefault<String>("compression_method",
                                                         collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");

    setURL(url_str);
}

void StorageHDFSConfiguration::setURL(const std::string & url_)
{
    auto pos = url_.find("//");
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}", url_);

    pos = url_.find('/', pos + 2);
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}", url_);

    path = url_.substr(pos + 1);
    if (!path.starts_with('/'))
        path = '/' + path;

    url = url_.substr(0, pos);
    paths = {path};

    LOG_TRACE(getLogger("StorageHDFSConfiguration"), "Using url: {}, path: {}", url, path);
}

void StorageHDFSConfiguration::addStructureAndFormatToArgs(
    ASTs & args,
    const String & structure_,
    const String & format_,
    ContextPtr context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        size_t count = args.size();
        if (count == 0 || count > 4)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Expected 1 to 4 arguments in table function, got {}", count);
        }

        auto format_literal = std::make_shared<ASTLiteral>(format_);
        auto structure_literal = std::make_shared<ASTLiteral>(structure_);

        /// hdfs(url)
        if (count == 1)
        {
            /// Add format=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        /// hdfs(url, format)
        else if (count == 2)
        {
            if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
                args.back() = format_literal;
            args.push_back(structure_literal);
        }
        /// hdfs(url, format, structure)
        /// hdfs(url, format, structure, compression_method)
        else if (count >= 3)
        {
            if (checkAndGetLiteralArgument<String>(args[1], "format") == "auto")
                args[1] = format_literal;
            if (checkAndGetLiteralArgument<String>(args[2], "structure") == "auto")
                args[2] = structure_literal;
        }
    }
}

}

#endif
