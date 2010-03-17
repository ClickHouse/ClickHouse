#ifndef DBMS_PARSERS_EXPRESSION_H
#define DBMS_PARSERS_EXPRESSION_H

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/variant/recursive_wrapper.hpp>
#include <boost/fusion/include/adapt_struct.hpp>

#include <iostream>

#include <DB/Core/Types.h>


namespace DB
{

/** Expression - это выражение, которое может стоять в запросе между SELECT и FROM
  * Здесь представлен парсер этого выражения (пока без операторов).
  * PS. Код довольно сложный :(
  */

/** Типы данных, которые образуются автоматически в результате парсинга.
  * Они должны быть в отдельном или глобальном namespace из-за BOOST_FUSION_ADAPT_STRUCT.
  *
  * spirit может собирать типы данных - результаты парсинга (атрибуты) автоматически,
  *  или можно использовать свои типы и заполнять их вручную с помощью семантических действий.
  * Здесь используется вариант - автоматически.
  *  (но стоит на будущее иметь ввиду, что его очень сложно заставить скомпилироваться)
  */
namespace ExpressionGrammarTypes
{
	/// с тем, какой конкретно тип у числа, будем разбираться потом
	typedef Float64 NumberConstant;									/// 1, -1, 0.1
	typedef String StringConstant;									/// 'abc', 'ab\'c', 'abc\\'
	typedef Null NullConstant;										/// NULL, null, NuLl

	typedef boost::variant<
		NumberConstant,
		StringConstant,
		NullConstant> Constant;

	typedef String Name;											/// x, sum, width

	struct Function;	/// forward declaration для рекурсии
	typedef boost::variant<
		boost::recursive_wrapper<Function>,
		Constant,
		Name> Node;													/// элемент выражения

	typedef std::vector<Node> Expression;							/// выражение - набор элементов через запятую

	struct Function													/// f(x), sum(a, 10), concat('a', 'bc')
	{
		Name name;													/// sum
		Expression args;											/// a, 10
	};
}

}


/** Необходимо, чтобы struct Function был совместим с boost::fusion::vector (он же boost::tuple),
  *  который образуется автоматически при парсинге.
  * Должно быть в глобальном namespace.
  */
BOOST_FUSION_ADAPT_STRUCT(
    DB::ExpressionGrammarTypes::Function,
    (DB::ExpressionGrammarTypes::Name, name)
	(DB::ExpressionGrammarTypes::Expression, args)
)


namespace DB
{

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;
namespace lambda = boost::lambda;


/** Грамматика */
template <typename Iterator>
struct ExpressionGrammar : qi::grammar<Iterator, ExpressionGrammarTypes::Expression(), ascii::space_type>
{
	ExpressionGrammar() : ExpressionGrammar::base_type(expression)
	{
		using qi::lexeme;
		using qi::raw;
		using qi::alpha;
		using qi::alnum;
		using qi::ulong_long;
		using qi::long_long;
		using qi::double_;
		using qi::eps;
		using qi::no_case;
		using qi::char_;
		using qi::_val;
		using qi::_1;
		
		expression %= node % ',';
		node %= function | constant | name;
		name %= raw[lexeme[alpha >> -*(alnum | '_')]];
		constant %= number | string | null;
		function %= name >> '(' >> expression >> ')';
		number %= double_;
		null = eps[_val = boost::none] >> no_case["NULL"];
		string %= raw[lexeme['\'' >> *((char_ - (char_('\\') | '\'')) | (char_('\'') >> char_)) >> '\'']];
	}

	qi::rule<Iterator, ExpressionGrammarTypes::Expression(), ascii::space_type> expression;
	qi::rule<Iterator, ExpressionGrammarTypes::Node(), ascii::space_type> node;
	qi::rule<Iterator, ExpressionGrammarTypes::Constant(), ascii::space_type> constant;
	qi::rule<Iterator, ExpressionGrammarTypes::Name(), ascii::space_type> name;
	qi::rule<Iterator, ExpressionGrammarTypes::Function(), ascii::space_type> function;
	qi::rule<Iterator, ExpressionGrammarTypes::NumberConstant(), ascii::space_type> number;
	qi::rule<Iterator, ExpressionGrammarTypes::StringConstant(), ascii::space_type> string;
	qi::rule<Iterator, ExpressionGrammarTypes::NullConstant(), ascii::space_type> null;
};

}

#endif
