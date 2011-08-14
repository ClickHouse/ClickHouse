#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Functions/FunctionsArithmetic.h>

#include <DB/Parsers/ParserSelectQuery.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		size_t n = argc == 2 ? atoi(argv[1]) : 10;
		
		DB::StorageSystemNumbers table;

		DB::Names column_names;
		column_names.push_back("number");

		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT number, number + 1, number * 2, number * 2 + 1";
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

		DB::Context context;
		context.columns["number"] = new DB::DataTypeUInt64;
		context.functions["plus"] = new DB::FunctionPlus;
		context.functions["multiply"] = new DB::FunctionMultiply;

		Poco::SharedPtr<DB::Expression> expression = new DB::Expression(ast, context);

		Poco::SharedPtr<DB::IBlockInputStream> in1(table.read(column_names, 0));

		Poco::SharedPtr<DB::ExpressionBlockInputStream> in2 = new DB::ExpressionBlockInputStream(in1, expression);
		DB::LimitBlockInputStream in3(in2, 10, std::max(0, static_cast<int>(n) - 10));
		
		DB::WriteBufferFromOStream out1(std::cout);
		DB::TabSeparatedRowOutputStream out2(out1, new DB::DataTypes(expression->getReturnTypes()));

		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			DB::copyData(in3, out2);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
