#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

String ASTFunction::getColumnNameImpl() const
{
    WriteBufferFromOwnString wb;
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
    return wb.str();
}

/** Get the text that identifies this element. */
String ASTFunction::getID() const
{
    return "Function_" + name;
}

ASTPtr ASTFunction::clone() const
{
    auto res = std::make_shared<ASTFunction>(*this);
    res->children.clear();

    if (arguments) { res->arguments = arguments->clone(); res->children.push_back(res->arguments); }
    if (parameters) { res->parameters = parameters->clone(); res->children.push_back(res->parameters); }

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

                    /** A particularly stupid case. If we have a unary minus before a literal that is a negative number
                        * "-(-1)" or "- -1", this can not be formatted as `--1`, since this will be interpreted as a comment.
                        * Instead, add a space.
                        * PS. You can not just ask to add parentheses - see formatImpl for ASTLiteral.
                        */
                    if (name == "negate" && typeid_cast<const ASTLiteral *>(&*arguments->children[0]))
                        settings.ostr << ' ';

                    arguments->formatImpl(settings, state, nested_need_parens);
                    written = true;
                }
            }
        }

        /** need_parens - do I need parentheses around the expression with the operator.
          * They are needed only if this expression is included in another expression with the operator.
          */

        if (!written && arguments->children.size() == 2)
        {
            const char * operators[] =
            {
                "multiply",        " * ",
                "divide",          " / ",
                "modulo",          " % ",
                "plus",            " + ",
                "minus",           " - ",
                "notEquals",       " != ",
                "lessOrEquals",    " <= ",
                "greaterOrEquals", " >= ",
                "less",            " < ",
                "greater",         " > ",
                "equals",          " = ",
                "like",            " LIKE ",
                "notLike",         " NOT LIKE ",
                "in",              " IN ",
                "notIn",           " NOT IN ",
                "globalIn",        " GLOBAL IN ",
                "globalNotIn",     " GLOBAL NOT IN ",
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
                /// It can be printed in a form of 'x.1' only if right hand side is unsigned integer literal.
                if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(arguments->children[1].get()))
                {
                    if (lit->value.getType() == Field::Types::UInt64)
                    {
                        arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                        settings.ostr << (settings.hilite ? hilite_operator : "") << "." << (settings.hilite ? hilite_none : "");
                        arguments->children[1]->formatImpl(settings, state, nested_need_parens);
                        written = true;
                    }
                }
            }

            if (!written && 0 == strcmp(name.c_str(), "lambda"))
            {
                /// Special case: one-element tuple in lhs of lambda is printed as its element.

                if (frame.need_parens)
                    settings.ostr << '(';

                const ASTFunction * first_arg_func = typeid_cast<const ASTFunction *>(arguments->children[0].get());
                if (first_arg_func
                    && first_arg_func->name == "tuple"
                    && first_arg_func->arguments
                    && first_arg_func->arguments->children.size() == 1)
                {
                    first_arg_func->arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                }
                else
                    arguments->children[0]->formatImpl(settings, state, nested_need_parens);

                settings.ostr << (settings.hilite ? hilite_operator : "") << " -> " << (settings.hilite ? hilite_none : "");
                arguments->children[1]->formatImpl(settings, state, nested_need_parens);
                if (frame.need_parens)
                    settings.ostr << ')';
                written = true;
            }
        }

        if (!written && arguments->children.size() >= 2)
        {
            const char * operators[] =
            {
                "and", " AND ",
                "or", " OR ",
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

        if (!written && 0 == strcmp(name.c_str(), "array"))
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
