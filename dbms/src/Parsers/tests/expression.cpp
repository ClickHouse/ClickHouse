#include <iostream>
#include <ostream>

#include <boost/variant/static_visitor.hpp>
#include <boost/variant/apply_visitor.hpp>

#include <DB/Parsers/ExpressionGrammar.h>


namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

typedef DB::ExpressionGrammarTypes::Expression Expr;

class dumpConst : public boost::static_visitor<>
{
public:
	void operator() (const DB::ExpressionGrammarTypes::NumberConstant & x) const
	{
		std::cout << "Number: " << x << std::endl;
	}

	void operator() (const DB::ExpressionGrammarTypes::StringConstant & x) const
	{
		std::cout << "String: " << x << std::endl;
	}

	void operator() (const DB::ExpressionGrammarTypes::NullConstant & x) const
	{
		std::cout << "Null" << std::endl;
	}
};

class dumpNode : public boost::static_visitor<>
{
public:
	void operator() (const DB::ExpressionGrammarTypes::Constant & x) const
	{
		boost::apply_visitor(dumpConst(), x);
	}

	void operator() (const DB::ExpressionGrammarTypes::Name & x) const
	{
		std::cout << "Name: " << x << std::endl;
	}

	void operator() (const DB::ExpressionGrammarTypes::Function & x) const
	{
		std::cout << "Function, name: " << x.name << ", args: " << std::endl;
		for (Expr::const_iterator it = x.args.begin(); it != x.args.end(); ++it)
			boost::apply_visitor(dumpNode(), *it);
		std::cout << std::endl;
	}
};

void dumpResult(const Expr & value)
{
	
	for (Expr::const_iterator it = value.begin(); it != value.end(); ++it)
		boost::apply_visitor(dumpNode(), *it);
}


int main(int argc, char ** argv)
{
	typedef std::string::const_iterator iterator_t;

	DB::ExpressionGrammar<iterator_t> parser;
	DB::ExpressionGrammarTypes::Expression value;

	std::string s = "ab1, b, f(c), 0, g(h(i), 1), -1, NULL, null, 'abc'";
	iterator_t it = s.begin();
	iterator_t end = s.end();

	bool res = boost::spirit::qi::phrase_parse(it, end, parser, ascii::space, value);

	std::cout << res << '\t' << s.substr(0, it - s.begin()) << std::endl;
	dumpResult(value);

	return 0;
}
