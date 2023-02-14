#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <fcntl.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/program_options.hpp>
#include <Common/scope_guard_safe.h>
#include "../client/Client.h"
#include "Core/Protocol.h"
#include "Parsers/formatAST.h"

#include <base/find_symbols.h>

#include <Access/AccessControl.h>

#include <Common/Config/configReadClient.h>
#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <Common/formatReadable.h>
#include "config_version.h"

#include <Columns/ColumnString.h>
#include <Core/QueryProcessingStage.h>
#include <Poco/Util/Application.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/UseSSL.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>

#include <Processors/Transforms/getSourceFromASTInsertQuery.h>

#include <Interpreters/InterpreterSetQuery.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>

#include <Client/ClientBase.h>
#include <Client/ClientBaseHelpers.h>
#include <Client/InternalTextLogs.h>
#include <Client/LineReader.h>
#include <Client/TestHint.h>
#include <Client/TestTags.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Protocol.h>
#include <Formats/FormatFactory.h>
#include <base/argsToConfig.h>
#include <base/safeExit.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/NetException.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TerminalSize.h>
#include <Common/UTF8Helpers.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/filesystemHelpers.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/scope_guard_safe.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <IO/CompressionMethod.h>
#include <IO/ForkWriteBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Access/AccessControl.h>
#include <Storages/ColumnsDescription.h>

#include <filesystem>
#include <iostream>
#include <map>
#include <unordered_map>
#include <boost/algorithm/string/case_conv.hpp>

#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "Top.h"
#include "config.h"
#include "config_version.h"

#ifndef __clang__
#    pragma GCC optimize("-fno-var-tracking-assignments")
#endif


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

namespace DB
{
int Top::start()
{
    // part of rewritten client function
    initialize(*this);

    UseSSL use_ssl;
    MainThreadStatus::getInstance();
    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    registerFormats();
    registerFunctions();
    registerAggregateFunctions();

    processConfig();

    connect();

    connection->setDefaultDatabase(connection_parameters.default_database);

    // my code
    is_interactive = false;
    String text = "SELECT value FROM system.metrics LIMIT 1 INTO OUTFILE 'top/out.txt' FORMAT TabSeparated";
    int i = 0;
    while (true)
    {
        remove("top/out.txt");
        processQueryText(text);
        String str;
        char c;
        ReadBufferFromFile read_buffer("top/out.txt");
        do
        {
            int status = read_buffer.read(c);
            if (status == read_buffer.eof())
            {
                break;
            }
            str.push_back(c);
        } while (c != '\n');


        std::cout << str << '\n';
        if (i == 10) {
            break;
        }
        ++i;
        read_buffer.close();
    }




    return 0;
}
}

int mainEntryClickHouseTop(int argc, char ** argv)
{
    DB::Top client;
    client.init(argc, argv);
    return client.start();
}
