#include <iostream>
#include <iomanip>

#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Functions/FunctionsLibrary.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Interpreters/Expression.h>


void dump(DB::IAST & ast, int level = 0)
{
	std::string prefix(level, ' ');
	
	if (DB::ASTFunction * node = dynamic_cast<DB::ASTFunction *>(&ast))
	{
		std::cout << prefix << node << " Function, name = " << node->function->getName()
			<< ", return type: " << node->return_type->getName() << std::endl;
	}
	else if (DB::ASTIdentifier * node = dynamic_cast<DB::ASTIdentifier *>(&ast))
	{
		std::cout << prefix << node << " Identifier, name = " << node->name << ", type = " << node->type->getName() << std::endl;
	}
	else if (DB::ASTLiteral * node = dynamic_cast<DB::ASTLiteral *>(&ast))
	{
		std::cout << prefix << node << " Literal, " << boost::apply_visitor(DB::FieldVisitorToString(), node->value)
			<< ", type = " << node->type->getName() << std::endl;
	}

	DB::ASTs children = ast.children;
	for (DB::ASTs::iterator it = children.begin(); it != children.end(); ++it)
		dump(**it, level + 1);
}


class OneBlockInputStream : public DB::IBlockInputStream
{
private:
	const DB::Block & block;
	bool has_been_read;
public:
	OneBlockInputStream(const DB::Block & block_) : block(block_), has_been_read(false) {}

	DB::Block read()
	{
		if (!has_been_read)
		{
			has_been_read = true;
			return block;
		}
		else
			return DB::Block();
	}

	DB::String getName() const { return "OneBlockInputStream"; }

	DB::BlockInputStreamPtr clone() { return new OneBlockInputStream(block); }
};


int main(int argc, char ** argv)
{
	try
	{
		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT x, s1, s2, "
			"/*"
			"2 + x * 2, x * 2, x % 3 == 1, "
			"s1 == 'abc', s1 == s2, s1 != 'abc', s1 != s2, "
			"s1 <  'abc', s1 <  s2, s1 >  'abc', s1 >  s2, "
			"s1 <= 'abc', s1 <= s2, s1 >= 'abc', s1 >= s2, "
			"*/"
			"s1 < s2 AND x % 3 < x % 5";
		std::string expected;

		const char * begin = input.data();
		const char * end = begin + input.size();
		const char * pos = begin;

		if (parser.parse(pos, end, ast, expected))
		{
			std::cout << "Success." << std::endl;
			DB::formatAST(*ast, std::cout);
			std::cout << std::endl << ast->getTreeID() << std::endl;
		}
		else
		{
			std::cout << "Failed at position " << (pos - begin) << ": "
				<< mysqlxx::quote << input.substr(pos - begin, 10)
				<< ", expected " << expected << "." << std::endl;
		}

		DB::Context context;
		context.columns.push_back(DB::NameAndTypePair("x", new DB::DataTypeInt16));
		context.columns.push_back(DB::NameAndTypePair("s1", new DB::DataTypeString));
		context.columns.push_back(DB::NameAndTypePair("s2", new DB::DataTypeString));

		context.functions = DB::FunctionsLibrary::get();
		
		DB::Expression expression(ast, context);

		dump(*ast);

		size_t n = argc == 2 ? atoi(argv[1]) : 10;

		DB::Block block;
		
		DB::ColumnWithNameAndType column_x;
		column_x.name = "x";
		column_x.type = new DB::DataTypeInt16;
		DB::ColumnInt16 * x = new DB::ColumnInt16;
		column_x.column = x;
		std::vector<Int16> & vec_x = x->getData();

		vec_x.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec_x[i] = i;

		block.insert(column_x);

		const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

		DB::ColumnWithNameAndType column_s1;
		column_s1.name = "s1";
		column_s1.type = new DB::DataTypeString;
		column_s1.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s1.column->insert(strings[i % 5]);

		block.insert(column_s1);

		DB::ColumnWithNameAndType column_s2;
		column_s2.name = "s2";
		column_s2.type = new DB::DataTypeString;
		column_s2.column = new DB::ColumnString;

		for (size_t i = 0; i < n; ++i)
			column_s2.column->insert(strings[i % 3]);

		block.insert(column_s2);

		{
			Poco::Stopwatch stopwatch;
			stopwatch.start();

			expression.execute(block);
			block = expression.projectResult(block);

			stopwatch.stop();
			std::cout << std::fixed << std::setprecision(2)
				<< "Elapsed " << stopwatch.elapsed() / 1000000.0 << " sec."
				<< ", " << n * 1000000 / stopwatch.elapsed() << " rows/sec."
				<< std::endl;
		}
		
		OneBlockInputStream * is = new OneBlockInputStream(block);
		DB::LimitBlockInputStream lis(is, 20, std::max(0, static_cast<int>(n) - 20));
		DB::WriteBufferFromOStream out_buf(std::cout);
		DB::TabSeparatedRowOutputStream os(out_buf, block);

		DB::copyData(lis, os);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.message() << std::endl;
	}

	return 0;
}
