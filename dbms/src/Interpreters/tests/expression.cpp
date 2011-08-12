#include <iostream>

#include <mysqlxx/mysqlxx.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Functions/FunctionsArithmetic.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>

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
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.message() << std::endl;
	}

	return 0;
}
