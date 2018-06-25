#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromOStream.h>
#include <IO/ReadHelpers.h>

#include <Storages/System/StorageSystemNumbers.h>

#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/copyData.h>

#include <DataTypes/DataTypesNumber.h>

#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>


int main(int argc, char ** argv)
try
{
    using namespace DB;

    size_t n = argc == 2 ? parse<UInt64>(argv[1]) : 10ULL;

    std::string input = "SELECT number, number / 3, number * number";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0);

    Context context = Context::createGlobal();

    ExpressionAnalyzer analyzer(ast, context, {}, {NameAndTypePair("number", std::make_shared<DataTypeUInt64>())});
    ExpressionActionsChain chain;
    analyzer.appendSelect(chain, false);
    analyzer.appendProjectResult(chain);
    chain.finalize();
    ExpressionActionsPtr expression = chain.getLastActions();

    StoragePtr table = StorageSystemNumbers::create("numbers", false);

    Names column_names;
    column_names.push_back("number");

    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);

    BlockInputStreamPtr in;
    in = table->read(column_names, {}, context, stage, 8192, 1)[0];
    in = std::make_shared<ExpressionBlockInputStream>(in, expression);
    in = std::make_shared<LimitBlockInputStream>(in, 10, std::max(static_cast<Int64>(0), static_cast<Int64>(n) - 10));

    WriteBufferFromOStream out1(std::cout);
    BlockOutputStreamPtr out = FormatFactory::instance().getOutput("TabSeparated", out1, expression->getSampleBlock(), context);

    {
        Stopwatch stopwatch;
        stopwatch.start();

        copyData(*in, *out);

        stopwatch.stop();
        std::cout << std::fixed << std::setprecision(2)
            << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
            << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
            << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}

