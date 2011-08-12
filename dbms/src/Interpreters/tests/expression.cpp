#include <iostream>

#include <mysqlxx/mysqlxx.h>

#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Functions/FunctionsArithmetic.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Interpreters/Expression.h>


void dump(DB::IAST & ast, int level = 0)
{
	std::string prefix(level, ' ');
	
	if (DB::ASTFunction * node = dynamic_cast<DB::ASTFunction *>(&ast))
	{
		std::cout << prefix << node << " Function, name = " << node->function->getName() << ", return types: ";
		for (DB::DataTypes::const_iterator it = node->return_types.begin(); it != node->return_types.end(); ++it)
		{
			if (it != node->return_types.begin())
				std::cout << ", ";
			std::cout << (*it)->getName();
		}
		std::cout << std::endl;
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

	DB::ASTs children = ast.getChildren();
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
};


int main(int argc, char ** argv)
{
	try
	{
		DB::ParserSelectQuery parser;
		DB::ASTPtr ast;
		std::string input = "SELECT 2 + x * 2, x * 2";
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
		context.columns["x"] = new DB::DataTypeInt16;
		context.functions["plus"] = new DB::FunctionPlus;
		context.functions["multiply"] = new DB::FunctionMultiply;

		DB::Expression expression(ast, context);

		dump(*ast);

		size_t n = 10;

		DB::Block block;
		DB::ColumnWithNameAndType column_x;
		column_x.name = "x";
		column_x.type = new DB::DataTypeInt16;
		DB::ColumnInt16 * x = new DB::ColumnInt16;
		column_x.column = x;
		std::vector<Int16> & vec = x->getData();

		vec.resize(n);
		for (size_t i = 0; i < n; ++i)
			vec[i] = i;

		block.insert(column_x);

		expression.execute(block);

		DB::DataTypes * data_types = new DB::DataTypes;
		for (size_t i = 0; i < block.columns(); ++i)
			data_types->push_back(block.getByPosition(i).type);

		OneBlockInputStream is(block);
		DB::WriteBufferFromOStream out_buf(std::cout);
		DB::TabSeparatedRowOutputStream os(out_buf, data_types);

		DB::copyData(is, os);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.message() << std::endl;
	}

	return 0;
}
