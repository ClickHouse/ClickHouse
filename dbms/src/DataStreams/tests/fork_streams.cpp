#include <iostream>
#include <iomanip>
#include <thread>

#include <IO/WriteBufferFromOStream.h>

#include <Storages/System/StorageSystemNumbers.h>

#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/ForkBlockInputStreams.h>
#include <DataStreams/copyData.h>

#include <DataTypes/DataTypesNumber.h>

#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>


void thread1(DB::BlockInputStreamPtr in, DB::BlockOutputStreamPtr out, DB::WriteBuffer & out_buf)
{
    while (DB::Block block = in->read())
    {
        out->write(block);
        out_buf.next();
    }
}

void thread2(DB::BlockInputStreamPtr in, DB::BlockOutputStreamPtr out, DB::WriteBuffer & out_buf)
{
    while (DB::Block block = in->read())
    {
        out->write(block);
        out_buf.next();
    }
}


int main(int argc, char ** argv)
try
{
    using namespace DB;

    std::string input = "SELECT number, number % 10000000 == 1";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

    formatAST(*ast, std::cerr);
    std::cerr << std::endl;

    Context context = Context::createGlobal();

    ExpressionAnalyzer analyzer(ast, context, {}, {NameAndTypePair("number", std::make_shared<DataTypeUInt64>())});
    ExpressionActionsChain chain;
    analyzer.appendSelect(chain, false);
    analyzer.appendProjectResult(chain, false);
    chain.finalize();
    ExpressionActionsPtr expression = chain.getLastActions();

    StoragePtr table = StorageSystemNumbers::create("numbers", false);

    Names column_names;
    column_names.push_back("number");

    QueryProcessingStage::Enum stage;

    BlockInputStreamPtr in = table->read(column_names, 0, context, stage, 8192, 1)[0];

    ForkBlockInputStreams fork(in);

    BlockInputStreamPtr in1 = fork.createInput();
    BlockInputStreamPtr in2 = fork.createInput();

    in1 = std::make_shared<FilterBlockInputStream>(in1, expression, 1);
    in1 = std::make_shared<LimitBlockInputStream>(in1, 10, 0);

    in2 = std::make_shared<FilterBlockInputStream>(in2, expression, 1);
    in2 = std::make_shared<LimitBlockInputStream>(in2, 20, 5);

    Block out_sample = expression->getSampleBlock();

    WriteBufferFromOStream ob1(std::cout);
    WriteBufferFromOStream ob2(std::cerr);

    BlockOutputStreamPtr out1 = context.getOutputFormat("TabSeparated", ob1, out_sample);
    BlockOutputStreamPtr out2 = context.getOutputFormat("TabSeparated", ob2, out_sample);

    std::thread thr1(std::bind(thread1, in1, out1, std::ref(ob1)));
    std::thread thr2(std::bind(thread2, in2, out2, std::ref(ob2)));

    fork.run();

    thr1.join();
    thr2.join();

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
