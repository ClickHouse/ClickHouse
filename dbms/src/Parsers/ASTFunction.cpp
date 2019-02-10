#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ASTFunction::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);

    if (parameters)
    {
        writeChar('(', ostr);
        for (auto it = parameters->children.begin(); it != parameters->children.end(); ++it)
        {
            if (it != parameters->children.begin())
                writeCString(", ", ostr);
            (*it)->appendColumnName(ostr);
        }
        writeChar(')', ostr);
    }

    writeChar('(', ostr);
    for (auto it = arguments->children.begin(); it != arguments->children.end(); ++it)
    {
        if (it != arguments->children.begin())
            writeCString(", ", ostr);
        (*it)->appendColumnName(ostr);
    }
    writeChar(')', ostr);
}

/** Get the text that identifies this element. */
String ASTFunction::getID(char delim) const
{
    return "Function" + (delim + name);
}

ASTPtr ASTFunction::clone() const
{
    auto res = std::make_shared<ASTFunction>(*this);
    res->children.clear();

    if (arguments) { res->arguments = arguments->clone(); res->children.push_back(res->arguments); }
    if (parameters) { res->parameters = parameters->clone(); res->children.push_back(res->parameters); }

    return res;
}


/** A special hack. If it's LIKE or NOT LIKE expression and the right hand side is a string literal,
  *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
  * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
  * With highlighting we make it clearly obvious.
  *
  * Another case is regexp match. Suppose the user types match(URL, 'www.yandex.ru'). It often means that the user is unaware that . is a metacharacter.
  */
static bool highlightStringLiteralWithMetacharacters(const ASTPtr & node, const IAST::FormatSettings & settings, const char * metacharacters)
{
    if (auto literal = dynamic_cast<const ASTLiteral *>(node.get()))
    {
        if (literal->value.getType() == Field::Types::String)
        {
            auto string = applyVisitor(FieldVisitorToString(), literal->value);

            unsigned escaping = 0;
            for (auto c : string)
            {
                if (c == '\\')
                {
                    settings.ostr << c;
                    if (escaping == 2)
                        escaping = 0;
                    ++escaping;
                }
                else if (nullptr != strchr(metacharacters, c))
                {
                    if (escaping == 2)      /// Properly escaped metacharacter
                        settings.ostr << c;
                    else                    /// Unescaped metacharacter
                        settings.ostr << "\033[1;35m" << c << "\033[0m";
                    escaping = 0;
                }
                else
                {
                    settings.ostr << c;
                    escaping = 0;
                }
            }

            return true;
        }
    }

    return false;
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

                    bool special_hilite = settings.hilite
                        && (name == "like" || name == "notLike")
                        && highlightStringLiteralWithMetacharacters(arguments->children[1], settings, "%_");

                    if (!special_hilite)
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

            bool special_hilite_regexp = settings.hilite
                && (name == "match" || name == "extract" || name == "extractAll" || name == "replaceRegexpOne" || name == "replaceRegexpAll");

            for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";

                bool special_hilite = false;
                if (i == 1 && special_hilite_regexp)
                    special_hilite = highlightStringLiteralWithMetacharacters(arguments->children[i], settings, "|()^$.[]?*+{:-");

                if (!special_hilite)
                    arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }

            settings.ostr << (settings.hilite ? hilite_function : "") << ')';
        }

        settings.ostr << (settings.hilite ? hilite_none : "");
    }
}

}
