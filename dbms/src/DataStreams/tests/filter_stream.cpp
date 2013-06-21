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
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		size_t n = argc == 2 ? DB::parse<UInt64>(argv[1]) : 10ULL;

		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT number, number % 3 == 1";
		std::string expected;

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
		std::cerr << std::endl;
		std::cerr << ast->getTreeID() << std::endl;

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

		Poco::SharedPtr<DB::IBlockInputStream> in = table->read(column_names, 0, DB::Settings(), stage)[0];
		in = new DB::ExpressionBlockInputStream(in, expression);
		in = new DB::FilterBlockInputStream(in, 1);
		in = new DB::LimitBlockInputStream(in, 10, std::max(static_cast<Int64>(0), static_cast<Int64>(n) - 10));
		
		DB::WriteBufferFromOStream ob(std::cout);
		DB::RowOutputStreamPtr out_ = new DB::TabSeparatedRowOutputStream(ob, expression->getSampleBlock());
		DB::BlockOutputStreamFromRowOutputStream out(out_);


		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			DB::copyData(*in, out);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
