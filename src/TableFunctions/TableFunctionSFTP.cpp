#include "registerTableFunctions.h"

#include <Storages/SFTP/StorageSFTP.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionSFTP.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int LOGICAL_ERROR;
    }

    StoragePtr TableFunctionSFTP::getStorage(
            const String & /* source */, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
            const std::string & table_name, const String & compression_method_) const
    {
        return std::make_shared<StorageSFTP>(
                configuration,
                StorageID(getDatabaseName(), table_name),
                format_,
                columns,
                ConstraintsDescription{},
                String{},
                global_context,
                compression_method_);
    }

    ColumnsDescription TableFunctionSFTP::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
    {
        if (structure == "auto")
        {
            std::shared_ptr<SshWrapper> ssh_wrapper;

            if (!configuration.password.empty()) {
                ssh_wrapper = std::make_shared<SshWrapper>(configuration.host, configuration.user, configuration.password, configuration.port);
            } else {
                ssh_wrapper = std::make_shared<SshWrapper>(configuration.host, configuration.user, configuration.port);
            }

            auto client = std::make_shared<SftpWrapper>(ssh_wrapper);

            String uri = "sftp://" + configuration.user + "@" + configuration.host + ":" + std::to_string(configuration.port);

            context->checkAccess(getSourceAccessType());
            return StorageSFTP::getTableStructureFromData(format, client, uri, configuration.path, compression_method, context);
        }

        return parseColumnsListFromString(structure, context);
    }

    void TableFunctionSFTP::parseArgumentsImpl(ASTs &args, const ContextPtr &context) {

        if (args.empty() || args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());
        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

        configuration.host = checkAndGetLiteralArgument<String>(args[0], "host");
        configuration.path = checkAndGetLiteralArgument<String>(args[1], "path");
        filename = configuration.path;
        configuration.user = checkAndGetLiteralArgument<String>(args[2], "user");

        String password = checkAndGetLiteralArgument<String>(args[3], "password");

        if (password == "DAEMON_AUTH")
        {
            configuration.password = "";
        }
        else
        {
            configuration.password = password;
        }

        UInt16 port = 22;

        size_t curr_arg = 4;

        if (args.size() > curr_arg) {
            String port_string = checkAndGetLiteralArgument<String>(args[3], "port");
            if (tryParse<UInt16>(port, port_string))
                ++curr_arg;
            else
                port = 22;
        }
        configuration.port = 22;

        if (args.size() > curr_arg)
        {
            args[curr_arg] = evaluateConstantExpressionOrIdentifierAsLiteral(args[curr_arg], context);
            format = checkAndGetLiteralArgument<String>(args[curr_arg], "format");
            ++curr_arg;
        }

        if (format == "auto")
            format = FormatFactory::instance().getFormatFromFileName(configuration.path, true);

        if (args.size() > curr_arg)
        {
            args[curr_arg] = evaluateConstantExpressionOrIdentifierAsLiteral(args[curr_arg], context);
            format = checkAndGetLiteralArgument<String>(args[curr_arg], "structure");
            ++curr_arg;
        }

        if (args.size() == curr_arg)
        {
            args[curr_arg] = evaluateConstantExpressionOrIdentifierAsLiteral(args[curr_arg], context);
            compression_method = checkAndGetLiteralArgument<String>(args[curr_arg], "compression_method");
        } else compression_method = "auto";

    }

    void registerTableFunctionSFTP(TableFunctionFactory & factory)
    {
        factory.registerFunction<TableFunctionSFTP>();
    }

}
