#include <iostream>
#include <iomanip>
#include <thread>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/ForkBlockInputStreams.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>


using Poco::SharedPtr;


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
{
	using namespace DB;

	try
	{
		std::string input = "SELECT number, number % 10000000 == 1";

		ParserSelectQuery parser;
		ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

		formatAST(*ast, std::cerr);
		std::cerr << std::endl;

		Context context;
		context.getColumns().push_back(NameAndTypePair("number", new DataTypeUInt64));

		ExpressionAnalyzer analyzer(ast, context, context.getColumns());
		ExpressionActionsChain chain;
		analyzer.appendSelect(chain, false);
		analyzer.appendProjectResult(chain, false);
		chain.finalize();
		ExpressionActionsPtr expression = chain.getLastActions();

		StoragePtr table = StorageSystemNumbers::create("Numbers");

		Names column_names;
		column_names.push_back("number");

		QueryProcessingStage::Enum stage;

		BlockInputStreamPtr in = table->read(column_names, 0, context, Settings(), stage)[0];

		ForkBlockInputStreams fork(in);

		BlockInputStreamPtr in1 = fork.createInput();
		BlockInputStreamPtr in2 = fork.createInput();

		in1 = new ExpressionBlockInputStream(in1, expression);
		in1 = new FilterBlockInputStream(in1, 1);
		in1 = new LimitBlockInputStream(in1, 10, 0);

		in2 = new ExpressionBlockInputStream(in2, expression);
		in2 = new FilterBlockInputStream(in2, 1);
		in2 = new LimitBlockInputStream(in2, 20, 5);

		Block out_sample = expression->getSampleBlock();
		FormatFactory format_factory;

		WriteBufferFromOStream ob1(std::cout);
		WriteBufferFromOStream ob2(std::cerr);

		BlockOutputStreamPtr out1 = format_factory.getOutput("TabSeparated", ob1, out_sample);
		BlockOutputStreamPtr out2 = format_factory.getOutput("TabSeparated", ob2, out_sample);

		std::thread thr1(std::bind(thread1, in1, out1, std::ref(ob1)));
		std::thread thr2(std::bind(thread2, in2, out2, std::ref(ob2)));

		fork.run();

		thr1.join();
		thr2.join();
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
