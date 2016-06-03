#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>

#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserCreateQuery.h>


namespace DB
{


const char * ParserMultiplicativeExpression::operators[] =
{
	"*", 	"multiply",
	"/", 	"divide",
	"%", 	"modulo",
	nullptr
};

const char * ParserUnaryMinusExpression::operators[] =
{
	"-", 	"negate",
	nullptr
};

const char * ParserAdditiveExpression::operators[] =
{
	"+", 	"plus",
	"-", 	"minus",
	nullptr
};

const char * ParserComparisonExpression::operators[] =
{
	"==", 		"equals",
	"!=", 		"notEquals",
	"<>", 		"notEquals",
	"<=", 		"lessOrEquals",
	">=", 		"greaterOrEquals",
	"<", 		"less",
	">", 		"greater",
	"=", 		"equals",
	"LIKE", 	"like",
	"NOT LIKE",	"notLike",
	"IN",		"in",
	"NOT IN",	"notIn",
	"GLOBAL IN",	"globalIn",
	"GLOBAL NOT IN","globalNotIn",
	nullptr
};

const char * ParserLogicalNotExpression::operators[] =
{
	"NOT", "not",
	nullptr
};

const char * ParserArrayElementExpression::operators[] =
{
	"[", 	"arrayElement",
	nullptr
};

const char * ParserTupleElementExpression::operators[] =
{
	".", 	"tupleElement",
	nullptr
};



bool ParserList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	bool first = true;
	ParserWhiteSpaceOrComments ws;

	ASTExpressionList * list = new ASTExpressionList;
	node = list;

	while (1)
	{
		if (first)
		{
			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
				break;

			list->children.push_back(elem);
			first = false;
		}
		else
		{
			ws.ignore(pos, end);
			if (!separator_parser->ignore(pos, end, max_parsed_pos, expected))
				break;
			ws.ignore(pos, end);

			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
				return false;

			list->children.push_back(elem);
		}
	}

	if (!allow_empty && first)
		return false;

	return true;
}


bool ParserLeftAssociativeBinaryOperatorList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	bool first = true;
	ParserWhiteSpaceOrComments ws;
	Pos begin = pos;

	while (1)
	{
		if (first)
		{
			ASTPtr elem;
			if (!first_elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
				return false;

			node = elem;
			first = false;
		}
		else
		{
			ws.ignore(pos, end);

			/// пробуем найти какой-нибудь из допустимых операторов

			const char ** it;
			for (it = operators; *it; it += 2)
			{
				ParserString op(it[0], true, true);
				if (op.ignore(pos, end, max_parsed_pos, expected))
					break;
			}

			if (!*it)
				break;

			ws.ignore(pos, end);

			/// функция, соответствующая оператору
			ASTFunction * p_function = new ASTFunction;
			ASTFunction & function = *p_function;
			ASTPtr function_node = p_function;

			/// аргументы функции
			ASTExpressionList * p_exp_list = new ASTExpressionList;
			ASTExpressionList & exp_list = *p_exp_list;
			ASTPtr exp_list_node = p_exp_list;

			ASTPtr elem;
			if (!(remaining_elem_parser ? remaining_elem_parser : first_elem_parser)->parse(pos, end, elem, max_parsed_pos, expected))
				return false;

			/// первым аргументом функции будет предыдущий элемент, вторым - следующий
			function.range.first = begin;
			function.range.second = pos;
			function.name = it[1];
			function.arguments = exp_list_node;
			function.children.push_back(exp_list_node);

			exp_list.children.push_back(node);
			exp_list.children.push_back(elem);
			exp_list.range.first = begin;
			exp_list.range.second = pos;

			/** специальное исключение для оператора доступа к элементу массива x[y], который
				* содержит инфиксную часть '[' и суффиксную ']' (задаётся в виде '[')
				*/
			if (0 == strcmp(it[0], "["))
			{
				ParserString rest_p("]");

				ws.ignore(pos, end);
				if (!rest_p.ignore(pos, end, max_parsed_pos, expected))
					return false;
			}

			node = function_node;
		}
	}

	return true;
}

bool ParserVariableArityOperatorList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;

	Pos begin = pos;
	ASTPtr arguments;

	if (!elem_parser->parse(pos, end, node, max_parsed_pos, expected))
		return false;

	while (true)
	{
		ws.ignore(pos, end);

		if (!infix_parser.ignore(pos, end, max_parsed_pos, expected))
			break;

		ws.ignore(pos, end);

		if (!arguments)
		{
			node = makeASTFunction(function_name, node);
			arguments = static_cast<ASTFunction &>(*node).arguments;
		}

		ASTPtr elem;
		if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
			return false;

		arguments->children.push_back(elem);
	}

	if (arguments)
		arguments->range = node->range = StringRange(begin, pos);

	return true;
}

bool ParserBetweenExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	/// Для выражения (subject BETWEEN left AND right)
	///  создаём AST такое же, как для (subject >= left AND subject <= right).

	ParserWhiteSpaceOrComments ws;
	ParserString s_between("BETWEEN", true, true);
	ParserString s_and("AND", true, true);

	ASTPtr subject;
	ASTPtr left;
	ASTPtr right;

	Pos begin = pos;

	if (!elem_parser.parse(pos, end, subject, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_between.ignore(pos, end, max_parsed_pos, expected))
		node = subject;
	else
	{
		ws.ignore(pos, end);

		if (!elem_parser.parse(pos, end, left, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!s_and.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!elem_parser.parse(pos, end, right, max_parsed_pos, expected))
			return false;

		/// функция AND
		std::unique_ptr<ASTFunction> f_and = std::make_unique<ASTFunction>();
		std::unique_ptr<ASTExpressionList> args_and = std::make_unique<ASTExpressionList>();

		/// >=
		std::unique_ptr<ASTFunction> f_ge = std::make_unique<ASTFunction>();
		std::unique_ptr<ASTExpressionList> args_ge = std::make_unique<ASTExpressionList>();

		/// <=
		std::unique_ptr<ASTFunction> f_le = std::make_unique<ASTFunction>();
		std::unique_ptr<ASTExpressionList> args_le = std::make_unique<ASTExpressionList>();

		args_ge->children.emplace_back(subject);
		args_ge->children.emplace_back(left);

		args_le->children.emplace_back(subject);
		args_le->children.emplace_back(right);

		f_ge->range.first = begin;
		f_ge->range.second = pos;
		f_ge->name = "greaterOrEquals";
		f_ge->arguments = args_ge.release();
		f_ge->children.emplace_back(f_ge->arguments);

		f_le->range.first = begin;
		f_le->range.second = pos;
		f_le->name = "lessOrEquals";
		f_le->arguments = args_le.release();
		f_le->children.emplace_back(f_le->arguments);

		args_and->children.emplace_back(f_ge.release());
		args_and->children.emplace_back(f_le.release());

		f_and->range.first = begin;
		f_and->range.second = pos;
		f_and->name = "and";
		f_and->arguments = args_and.release();
		f_and->children.emplace_back(f_and->arguments);

		node = f_and.release();
	}

	return true;
}

bool ParserTernaryOperatorExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;
	ParserString symbol1("?");
	ParserString symbol2(":");

	ASTPtr elem_cond;
	ASTPtr elem_then;
	ASTPtr elem_else;

	Pos begin = pos;

	if (!elem_parser.parse(pos, end, elem_cond, max_parsed_pos, expected))
		return false;

	ws.ignore(pos, end);

	if (!symbol1.ignore(pos, end, max_parsed_pos, expected))
		node = elem_cond;
	else
	{
		ws.ignore(pos, end);

		if (!elem_parser.parse(pos, end, elem_then, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!symbol2.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!elem_parser.parse(pos, end, elem_else, max_parsed_pos, expected))
			return false;

		/// функция, соответствующая оператору
		ASTFunction * p_function = new ASTFunction;
		ASTFunction & function = *p_function;
		ASTPtr function_node = p_function;

		/// аргументы функции
		ASTExpressionList * p_exp_list = new ASTExpressionList;
		ASTExpressionList & exp_list = *p_exp_list;
		ASTPtr exp_list_node = p_exp_list;

		function.range.first = begin;
		function.range.second = pos;
		function.name = "if";
		function.arguments = exp_list_node;
		function.children.push_back(exp_list_node);

		exp_list.children.push_back(elem_cond);
		exp_list.children.push_back(elem_then);
		exp_list.children.push_back(elem_else);
		exp_list.range.first = begin;
		exp_list.range.second = pos;

		node = function_node;
	}

	return true;
}


bool ParserLambdaExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;
	ParserString arrow("->");
	ParserString open("(");
	ParserString close(")");

	Pos begin = pos;

	do
	{
		ASTPtr inner_arguments;
		ASTPtr expression;

		bool was_open = false;

		if (open.ignore(pos, end, max_parsed_pos, expected))
		{
			ws.ignore(pos, end, max_parsed_pos, expected);
			was_open = true;
		}

		if (!ParserList(ParserPtr(new ParserIdentifier), ParserPtr(new ParserString(","))).parse(pos, end, inner_arguments, max_parsed_pos, expected))
			break;
		ws.ignore(pos, end, max_parsed_pos, expected);

		if (was_open)
		{
			if (!close.ignore(pos, end, max_parsed_pos, expected))
				break;
			ws.ignore(pos, end, max_parsed_pos, expected);
		}

		if (!arrow.ignore(pos, end, max_parsed_pos, expected))
			break;
		ws.ignore(pos, end, max_parsed_pos, expected);

		if (!elem_parser.parse(pos, end, expression, max_parsed_pos, expected))
			return false;

		/// lambda(tuple(inner_arguments), expression)

		ASTFunction * lambda = new ASTFunction;
		node = lambda;
		lambda->name = "lambda";

		ASTExpressionList * outer_arguments = new ASTExpressionList;
		lambda->arguments = outer_arguments;
		lambda->children.push_back(lambda->arguments);

		ASTFunction * tuple = new ASTFunction;
		outer_arguments->children.push_back(tuple);
		tuple->name = "tuple";
		tuple->arguments = inner_arguments;
		tuple->children.push_back(inner_arguments);

		outer_arguments->children.push_back(expression);

		return true;
	}
	while (false);

	pos = begin;
	return elem_parser.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserPrefixUnaryOperatorExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserWhiteSpaceOrComments ws;

	/// пробуем найти какой-нибудь из допустимых операторов
	Pos begin = pos;
	const char ** it;
	for (it = operators; *it; it += 2)
	{
		ParserString op(it[0], true, true);
		if (op.ignore(pos, end, max_parsed_pos, expected))
			break;
	}

	ws.ignore(pos, end);

	/// Позволяем парсить цепочки вида NOT NOT x. Это хак.
	/** Так сделано, потому что среди унарных операторов есть только минус и NOT.
	  * Но для минуса цепочку из унарных операторов не требуется поддерживать.
	  */
	if (it[0] && 0 == strncmp(it[0], "NOT", 3))
	{
		/// Было ли чётное количество NOT.
		bool even = false;

		const char ** jt;
		while (true)
		{
			for (jt = operators; *jt; jt += 2)
			{
				ParserString op(jt[0], true, true);
				if (op.ignore(pos, end, max_parsed_pos, expected))
					break;
			}

			if (!*jt)
				break;

			even = !even;

			ws.ignore(pos, end);
		}

		if (even)
			it = jt;	/// Зануляем результат парсинга первого NOT. Получается, как будто цепочки NOT нет вообще.
	}

	ASTPtr elem;
	if (!elem_parser->parse(pos, end, elem, max_parsed_pos, expected))
		return false;

	if (!*it)
		node = elem;
	else
	{
		/// функция, соответствующая оператору
		ASTFunction * p_function = new ASTFunction;
		ASTFunction & function = *p_function;
		ASTPtr function_node = p_function;

		/// аргументы функции
		ASTExpressionList * p_exp_list = new ASTExpressionList;
		ASTExpressionList & exp_list = *p_exp_list;
		ASTPtr exp_list_node = p_exp_list;

		function.range.first = begin;
		function.range.second = pos;
		function.name = it[1];
		function.arguments = exp_list_node;
		function.children.push_back(exp_list_node);

		exp_list.children.push_back(elem);
		exp_list.range.first = begin;
		exp_list.range.second = pos;

		node = function_node;
	}

	return true;
}


bool ParserUnaryMinusExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	/// В качестве исключения, отрицательные числа должны парситься, как литералы, а не как применение оператора.

	if (pos < end && *pos == '-')
	{
		ParserLiteral lit_p;
		Pos begin = pos;

		if (lit_p.parse(pos, end, node, max_parsed_pos, expected))
			return true;

		pos = begin;
	}

	return operator_parser.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserArrayElementExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected &expected)
{
	return ParserLeftAssociativeBinaryOperatorList{
		operators,
		ParserPtr(new ParserExpressionElement),
		ParserPtr(new ParserExpressionWithOptionalAlias(false))
	}.parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserTupleElementExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected &expected)
{
	return ParserLeftAssociativeBinaryOperatorList{
		operators,
		ParserPtr(new ParserArrayElementExpression),
		ParserPtr(new ParserUnsignedInteger)
	}.parse(pos, end, node, max_parsed_pos, expected);
}


ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword)
	: impl(new ParserWithOptionalAlias(ParserPtr(new ParserLambdaExpression), allow_alias_without_as_keyword))
{
}


ParserExpressionInCastExpression::ParserExpressionInCastExpression(bool allow_alias_without_as_keyword)
	: impl(new ParserCastExpressionWithOptionalAlias(ParserPtr(new ParserLambdaExpression), allow_alias_without_as_keyword))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	return ParserList(ParserPtr(new ParserExpressionWithOptionalAlias(allow_alias_without_as_keyword)), ParserPtr(new ParserString(","))).parse(pos, end, node, max_parsed_pos, expected);
}


bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	return nested_parser.parse(pos, end, node, max_parsed_pos, expected)
		&& !typeid_cast<ASTExpressionList &>(*node).children.empty();
}


bool ParserOrderByExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	return ParserList(ParserPtr(new ParserOrderByElement), ParserPtr(new ParserString(",")), false).parse(pos, end, node, max_parsed_pos, expected);
}


}
