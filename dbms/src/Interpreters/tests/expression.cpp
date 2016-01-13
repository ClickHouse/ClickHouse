#include <iostream>
#include <iomanip>

#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Columns/ColumnString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		std::string input = "SELECT x, s1, s2, "
			"/*"
			"2 + x * 2, x * 2, x % 3 == 1, "
			"s1 == 'abc', s1 == s2, s1 != 'abc', s1 != s2, "
			"s1 <  'abc', s1 <  s2, s1 >  'abc', s1 >  s2, "
			"s1 <= 'abc', s1 <= s2, s1 >= 'abc', s1 >= s2, "
			"*/"
			"s1 < s2 AND x % 3 < x % 5";

		ParserSelectQuery parser;
		ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

		formatAST(*ast, std::cerr);
		std::cerr << std::endl;

		Context context;
		NamesAndTypesList columns
		{
			{"x", new DataTypeInt16},
			{"s1", new DataTypeString},
			{"s2", new DataTypeString}
		};

		ExpressionAnalyzer analyzer(ast, context, {}, columns);
		ExpressionActionsChain chain;
		analyzer.appendSelect(chain, false);
		analyzer.appendProjectResult(chain, false);
		chain.finalize();
		ExpressionActionsPtr expression = chain.getLastActions();

		size_t n = argc == 2 ? atoi(argv[1]) : 10;

		Block block;

		ColumnWithTypeAndName column_x;
		column_x.name = "x";
		column_x.type = new DataTypeInt16;
		ColumnInt16 * x = new ColumnInt16;
		column_x.column = x;
		auto & vec_x = x->getData();

		vec_x.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec_x[i] = i;

		block.insert(column_x);

		const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

		ColumnWithTypeAndName column_s1;
		column_s1.name = "s1";
		column_s1.type = new DataTypeString;
		column_s1.column = new ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s1.column->insert(String(strings[i % 5]));

		block.insert(column_s1);

		ColumnWithTypeAndName column_s2;
		column_s2.name = "s2";
		column_s2.type = new DataTypeString;
		column_s2.column = new ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s2.column->insert(String(strings[i % 3]));

		block.insert(column_s2);

		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			expression->execute(block);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}

		OneBlockInputStream * is = new OneBlockInputStream(block);
		LimitBlockInputStream lis(is, 20, std::max(0, static_cast<int>(n) - 20));
		WriteBufferFromOStream out_buf(std::cout);
		RowOutputStreamPtr os_ = new TabSeparatedRowOutputStream(out_buf, block);
		BlockOutputStreamFromRowOutputStream os(os_);

		copyData(lis, os);
	}
	catch (const Exception & e)
	{
		std::cerr << e.displayText() << std::endl;
	}

	return 0;
}
