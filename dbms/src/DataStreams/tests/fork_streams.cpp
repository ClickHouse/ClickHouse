#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/ForkBlockInputStreams.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Parsers/ParserSelectQuery.h>
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
	try
	{
		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT number, number % 10000000 == 1";
		const char * expected = "";

		const char * begin = input.data();
		const char * end = begin + input.size();
		const char * pos = begin;

		if (!parser.parse(pos, end, ast, expected))
		{
			std::cout << "Failed at position " << (pos - begin) << ": "
				<< mysqlxx::quote << input.substr(pos - begin, 10)
				<< ", expected " << expected << "." << std::endl;
		}

		DB::formatAST(*ast, std::cerr);

		DB::Context context;
		context.getColumns().push_back(DB::NameAndTypePair("number", new DB::DataTypeUInt64));

		DB::ExpressionAnalyzer analyzer(ast, context);
		DB::ExpressionActionsChain chain;
		analyzer.appendSelect(chain);
		analyzer.appendProjectResult(chain);
		chain.finalize();
		DB::ExpressionActionsPtr expression = chain.getLastActions();

		DB::StoragePtr table = DB::StorageSystemNumbers::create("Numbers");

		DB::Names column_names;
		column_names.push_back("number");

		DB::QueryProcessingStage::Enum stage;

		DB::BlockInputStreamPtr in = table->read(column_names, 0, DB::Settings(), stage)[0];

		DB::ForkBlockInputStreams fork(in);

		DB::BlockInputStreamPtr in1 = fork.createInput();
		DB::BlockInputStreamPtr in2 = fork.createInput();

		in1 = new DB::ExpressionBlockInputStream(in1, expression);
		in1 = new DB::FilterBlockInputStream(in1, 1);
		in1 = new DB::LimitBlockInputStream(in1, 10, 0);

		in2 = new DB::ExpressionBlockInputStream(in2, expression);
		in2 = new DB::FilterBlockInputStream(in2, 1);
		in2 = new DB::LimitBlockInputStream(in2, 20, 5);

		DB::Block out_sample = expression->getSampleBlock();
		DB::FormatFactory format_factory;

		DB::WriteBufferFromOStream ob1(std::cout);
		DB::WriteBufferFromOStream ob2(std::cerr);

		DB::BlockOutputStreamPtr out1 = format_factory.getOutput("TabSeparated", ob1, out_sample);
		DB::BlockOutputStreamPtr out2 = format_factory.getOutput("TabSeparated", ob2, out_sample);

		boost::thread thr1(thread1, in1, out1, boost::ref(ob1));
		boost::thread thr2(thread2, in2, out2, boost::ref(ob2));

		fork.run();

		thr1.join();
		thr2.join();
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
