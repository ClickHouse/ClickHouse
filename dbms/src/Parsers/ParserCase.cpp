#include <DB/Parsers/ParserCase.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Core/Field.h>

namespace DB
{

bool ParserCase::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_case{"CASE", true, true};
	ParserString s_when{"WHEN", true, true};
	ParserString s_then{"THEN", true, true};
	ParserString s_else{"ELSE", true, true};
	ParserString s_end{ "END",  true, true};
	ParserExpressionWithOptionalAlias p_expr{false};

	if (!s_case.parse(pos, end, node, max_parsed_pos, expected))
	{
		/// Parse as a simple ASTFunction.
		return ParserFunction{}.parse(pos = begin, end, node, max_parsed_pos, expected);
	}

	ws.ignore(pos, end);

	bool has_case_expr = false;

	auto old_pos = pos;
	has_case_expr = !s_when.parse(pos, end, node, max_parsed_pos, expected);
	pos = old_pos;

	ASTs args;

	using ASTFunctionPtr = Poco::SharedPtr<ASTFunction>;
	using ASTExpressionListPtr = Poco::SharedPtr<ASTExpressionList>;

	auto parse_branches = [&]()
	{
		bool has_branch = false;
		while (s_when.parse(pos, end, node, max_parsed_pos, expected))
		{
			has_branch = true;

			ws.ignore(pos, end);

			ASTPtr expr_when;
			if (!p_expr.parse(pos, end, expr_when, max_parsed_pos, expected))
				return false;
			args.push_back(expr_when);

			ws.ignore(pos, end);

			if (!s_then.parse(pos, end, node, max_parsed_pos, expected))
				return false;

			ws.ignore(pos, end);

			ASTPtr expr_then;
			if (!p_expr.parse(pos, end, expr_then, max_parsed_pos, expected))
				return false;
			args.push_back(expr_then);

			ws.ignore(pos, end);
		}

		if (!has_branch)
			return false;

		ws.ignore(pos, end);

		if (!s_else.parse(pos, end, node, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		ASTPtr expr_else;
		if (!p_expr.parse(pos, end, expr_else, max_parsed_pos, expected))
			return false;
		args.push_back(expr_else);

		ws.ignore(pos, end);

		if (!s_end.parse(pos, end, node, max_parsed_pos, expected))
			return false;

		return true;
	};

	if (has_case_expr)
	{
		ASTPtr case_expr;
		if (!p_expr.parse(pos, end, case_expr, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!parse_branches())
			return false;

		/// Hand-craft a transform() function.
		ASTExpressionListPtr src_expr_list = new ASTExpressionList{StringRange{begin, pos}};
		ASTExpressionListPtr dst_expr_list = new ASTExpressionList{StringRange{begin, pos}};

		for (size_t i = 0; i < (args.size() - 1); ++i)
		{
			if ((i % 2) == 0)
				src_expr_list->children.push_back(args[i]);
			else
				dst_expr_list->children.push_back(args[i]);
		}

		ASTFunctionPtr src_array_function = new ASTFunction{StringRange{begin, pos}};
		src_array_function->name = "array";
		src_array_function->genus = ASTFunction::Genus::CASE_ARRAY;
		src_array_function->arguments = src_expr_list;
		src_array_function->children.push_back(src_array_function->arguments);

		ASTFunctionPtr dst_array_function = new ASTFunction{StringRange{begin, pos}};
		dst_array_function->name = "array";
		dst_array_function->genus = ASTFunction::Genus::CASE_ARRAY;
		dst_array_function->arguments = dst_expr_list;
		dst_array_function->children.push_back(dst_array_function->arguments);

		ASTPtr function_args = new ASTExpressionList{StringRange{begin, pos}};
		function_args->children.push_back(case_expr);
		function_args->children.push_back(src_array_function);
		function_args->children.push_back(dst_array_function);
		function_args->children.emplace_back(args.back());

		ASTFunctionPtr function = new ASTFunction{StringRange{begin, pos}};
		function->name = "transform";
		function->genus = ASTFunction::Genus::CASE_WITH_EXPR;
		function->arguments = function_args;
		function->children.push_back(function->arguments);

		node = function;
	}
	else
	{
		if (!parse_branches())
			return false;

		/// Hand-craft a multiIf() function.
		ASTPtr function_args = new ASTExpressionList{StringRange{begin, pos}};
		function_args->children = std::move(args);

		ASTFunctionPtr function = new ASTFunction{StringRange{begin, pos}};
		function->name = "multiIf";
		function->genus = ASTFunction::Genus::CASE_WITHOUT_EXPR;
		function->arguments = function_args;
		function->children.push_back(function->arguments);

		node = function;
	}

	return true;
}

}
