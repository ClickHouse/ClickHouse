#include <DB/Common/typeid_cast.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{

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

	/// Should this function to be written as operator?
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

}
