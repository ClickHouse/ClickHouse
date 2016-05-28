#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{

extern const int INVALID_FUNCTION_GENUS;

}

String ASTFunction::getColumnName() const
{
	String res;
	WriteBufferFromString wb(res);
	writeString(name, wb);

	if (parameters)
	{
		writeChar('(', wb);
		for (ASTs::const_iterator it = parameters->children.begin(); it != parameters->children.end(); ++it)
		{
			if (it != parameters->children.begin())
				writeCString(", ", wb);
			writeString((*it)->getColumnName(), wb);
		}
		writeChar(')', wb);
	}

	writeChar('(', wb);
	for (ASTs::const_iterator it = arguments->children.begin(); it != arguments->children.end(); ++it)
	{
		if (it != arguments->children.begin())
			writeCString(", ", wb);
		writeString((*it)->getColumnName(), wb);
	}
	writeChar(')', wb);

	return res;
}

/** Получить текст, который идентифицирует этот элемент. */
String ASTFunction::getID() const
{
	return "Function_" + name;
}

ASTPtr ASTFunction::clone() const
{
	auto res = std::make_shared<ASTFunction>(*this);
	res->children.clear();

	if (arguments) 	{ res->arguments = arguments->clone();		res->children.push_back(res->arguments); }
	if (parameters) { res->parameters = parameters->clone(); 	res->children.push_back(res->parameters); }

	return res;
}

void ASTFunction::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	FormatStateStacked nested_need_parens = frame;
	FormatStateStacked nested_dont_need_parens = frame;
	nested_need_parens.need_parens = true;
	nested_dont_need_parens.need_parens = false;

	if ((genus == Genus::CASE_WITH_EXPR) || (genus == Genus::CASE_WITHOUT_EXPR))
	{
		formatCase(settings, state, frame);
		return;
	}

	/// Стоит ли записать эту функцию в виде оператора?
	bool written = false;
	if (arguments && !parameters)
	{
		if (0 == strcmp(name.data(), "CAST"))
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << name;

			settings.ostr << '(' << (settings.hilite ? hilite_none : "");

			arguments->children.front()->formatImpl(settings, state, nested_need_parens);

			settings.ostr <<  (settings.hilite ? hilite_keyword : "") << " AS "
				<< (settings.hilite ? hilite_none : "");

			settings.ostr << (settings.hilite ? hilite_function : "")
				<< typeid_cast<const ASTLiteral &>(*arguments->children.back()).value.safeGet<String>()
				<< (settings.hilite ? hilite_none : "");

			settings.ostr << (settings.hilite ? hilite_keyword : "") << ')'
				<< (settings.hilite ? hilite_none : "");

			written = true;
		}

		if (arguments->children.size() == 1)
		{
			const char * operators[] =
			{
				"negate", "-",
				"not", "NOT ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(name.c_str(), func[0]))
				{
					settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");

					/** Особо дурацкий случай. Если у нас унарный минус перед литералом, являющимся отрицательным числом:
						* "-(-1)" или "- -1", то это нельзя форматировать как --1, так как это будет воспринято как комментарий.
						* Вместо этого, добавим пробел.
						* PS. Нельзя просто попросить добавить скобки - см. formatImpl для ASTLiteral.
						*/
					if (name == "negate" && typeid_cast<const ASTLiteral *>(&*arguments->children[0]))
						settings.ostr << ' ';

					arguments->formatImpl(settings, state, nested_need_parens);
					written = true;
				}
			}
		}

		/** need_parens - нужны ли скобки вокруг выражения с оператором.
			* Они нужны, только если это выражение входит в другое выражение с оператором.
			*/

		if (!written && arguments->children.size() == 2)
		{
			const char * operators[] =
			{
				"multiply",			" * ",
				"divide",			" / ",
				"modulo",			" % ",
				"plus", 			" + ",
				"minus", 			" - ",
				"notEquals",		" != ",
				"lessOrEquals",		" <= ",
				"greaterOrEquals",	" >= ",
				"less",				" < ",
				"greater",			" > ",
				"equals",			" = ",
				"like",				" LIKE ",
				"notLike",			" NOT LIKE ",
				"in",				" IN ",
				"notIn",			" NOT IN ",
				"globalIn",			" GLOBAL IN ",
				"globalNotIn",		" GLOBAL NOT IN ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(name.c_str(), func[0]))
				{
					if (frame.need_parens)
						settings.ostr << '(';
					arguments->children[0]->formatImpl(settings, state, nested_need_parens);
					settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
					arguments->children[1]->formatImpl(settings, state, nested_need_parens);
					if (frame.need_parens)
						settings.ostr << ')';
					written = true;
				}
			}

			if (!written && 0 == strcmp(name.c_str(), "arrayElement"))
			{
				arguments->children[0]->formatImpl(settings, state, nested_need_parens);
				settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
				arguments->children[1]->formatImpl(settings, state, nested_need_parens);
				settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
				written = true;
			}

			if (!written && 0 == strcmp(name.c_str(), "tupleElement"))
			{
				arguments->children[0]->formatImpl(settings, state, nested_need_parens);
				settings.ostr << (settings.hilite ? hilite_operator : "") << "." << (settings.hilite ? hilite_none : "");
				arguments->children[1]->formatImpl(settings, state, nested_need_parens);
				written = true;
			}
		}

		if (!written && arguments->children.size() >= 2)
		{
			const char * operators[] =
			{
				"and",				" AND ",
				"or",				" OR ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(name.c_str(), func[0]))
				{
					if (frame.need_parens)
						settings.ostr << '(';
					for (size_t i = 0; i < arguments->children.size(); ++i)
					{
						if (i != 0)
							settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
						arguments->children[i]->formatImpl(settings, state, nested_need_parens);
					}
					if (frame.need_parens)
						settings.ostr << ')';
					written = true;
				}
			}
		}

		if (!written && arguments->children.size() >= 1 && 0 == strcmp(name.c_str(), "array"))
		{
			settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
			for (size_t i = 0; i < arguments->children.size(); ++i)
			{
				if (i != 0)
					settings.ostr << ", ";
				arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
			}
			settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
			written = true;
		}

		if (!written && arguments->children.size() >= 2 && 0 == strcmp(name.c_str(), "tuple"))
		{
			settings.ostr << (settings.hilite ? hilite_operator : "") << '(' << (settings.hilite ? hilite_none : "");
			for (size_t i = 0; i < arguments->children.size(); ++i)
			{
				if (i != 0)
					settings.ostr << ", ";
				arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
			}
			settings.ostr << (settings.hilite ? hilite_operator : "") << ')' << (settings.hilite ? hilite_none : "");
			written = true;
		}
	}

	if (!written)
	{
		settings.ostr << (settings.hilite ? hilite_function : "") << name;

		if (parameters)
		{
			settings.ostr << '(' << (settings.hilite ? hilite_none : "");
			parameters->formatImpl(settings, state, nested_dont_need_parens);
			settings.ostr << (settings.hilite ? hilite_function : "") << ')';
		}

		if (arguments)
		{
			settings.ostr << '(' << (settings.hilite ? hilite_none : "");
			arguments->formatImpl(settings, state, nested_dont_need_parens);
			settings.ostr << (settings.hilite ? hilite_function : "") << ')';
		}

		settings.ostr << (settings.hilite ? hilite_none : "");
	}
}

void ASTFunction::formatCase(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	static constexpr auto s_case = "CASE";
	static constexpr auto s_when = "WHEN";
	static constexpr auto s_then = "THEN";
	static constexpr auto s_else = "ELSE";
	static constexpr auto s_end = "END";
	static constexpr auto s_ws = " ";

	const ASTExpressionList * expr_list = static_cast<const ASTExpressionList *>(&*arguments);
	const ASTs & args = expr_list->children;

	frame.need_parens = false;

	std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
	std::string indent_str2 = settings.one_line ? "" : std::string(4 * (frame.indent + 1), ' ');

	settings.ostr << (settings.hilite ? hilite_keyword : "") << s_case << s_ws
		<< (settings.hilite ? hilite_none : "");

	if (genus == Genus::CASE_WITH_EXPR)
	{
		args[0]->formatImpl(settings, state, frame);
		settings.ostr << settings.nl_or_ws;

		const ASTFunction * src_array_function = static_cast<const ASTFunction *>(&*args[1]);
		const ASTExpressionList * src_expr_list = static_cast<const ASTExpressionList *>(&*src_array_function->arguments);

		const ASTFunction * dst_array_function = static_cast<const ASTFunction *>(&*args[2]);
		const ASTExpressionList * dst_expr_list = static_cast<const ASTExpressionList *>(&*dst_array_function->arguments);

		size_t size = src_expr_list->children.size();

		for (size_t i = 0; i < size; ++i)
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "")
				<< indent_str2 << s_when << s_ws;
			src_expr_list->children[i]->formatImpl(settings, state, frame);
			settings.ostr << s_ws;

			settings.ostr << (settings.hilite ? hilite_keyword : "") << s_then << s_ws;
			settings.ostr << hilite_none;
			dst_expr_list->children[i]->formatImpl(settings, state, frame);
			settings.ostr << settings.nl_or_ws;
		}
	}
	else if (genus == Genus::CASE_WITHOUT_EXPR)
	{
		settings.ostr << settings.nl_or_ws;

		for (size_t i = 0; i < (args.size() - 1); ++i)
		{
			if ((i % 2) == 0)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "")
					<< indent_str2 << s_when << s_ws;
				args[i]->formatImpl(settings, state, frame);
				settings.ostr << " ";
			}
			else
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << s_then << s_ws;
				settings.ostr << hilite_none;
				args[i]->formatImpl(settings, state, frame);
				settings.ostr << settings.nl_or_ws;
			}
		}
	}
	else
		throw Exception{"Invalid function genus", ErrorCodes::INVALID_FUNCTION_GENUS};

	settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str2
		<< s_else << s_ws;
	settings.ostr << hilite_none;
	args.back()->formatImpl(settings, state, frame);
	settings.ostr << settings.nl_or_ws;

	settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << s_end;
}

}
