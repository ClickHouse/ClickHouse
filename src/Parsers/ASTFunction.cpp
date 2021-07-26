#include <Parsers/ASTFunction.h>

#include <Common/quoteString.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NumberTraits.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_EXPRESSION;
}

void ASTFunction::appendColumnNameImpl(WriteBuffer & ostr) const
{
    if (name == "view")
        throw Exception("Table function view cannot be used as an expression", ErrorCodes::UNEXPECTED_EXPRESSION);

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
    if (arguments)
        for (auto it = arguments->children.begin(); it != arguments->children.end(); ++it)
        {
            if (it != arguments->children.begin())
                writeCString(", ", ostr);
            (*it)->appendColumnName(ostr);
        }
    writeChar(')', ostr);

    if (is_window_function)
    {
        writeCString(" OVER ", ostr);
        if (!window_name.empty())
        {
            ostr << window_name;
        }
        else
        {
            FormatSettings settings{ostr, true /* one_line */};
            FormatState state;
            FormatStateStacked frame;
            writeCString("(", ostr);
            window_definition->formatImpl(settings, state, frame);
            writeCString(")", ostr);
        }
    }
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

    if (window_definition)
    {
        res->window_definition = window_definition->clone();
        res->children.push_back(res->window_definition);
    }

    return res;
}


void ASTFunction::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    IAST::updateTreeHashImpl(hash_state);
}


ASTPtr ASTFunction::toLiteral() const
{
    if (!arguments) return {};

    if (name == "array")
    {
        Array array;

        for (const auto & arg : arguments->children)
        {
            if (auto * literal = arg->as<ASTLiteral>())
                array.push_back(literal->value);
            else if (auto * func = arg->as<ASTFunction>())
            {
                if (auto func_literal = func->toLiteral())
                    array.push_back(func_literal->as<ASTLiteral>()->value);
            }
            else
                /// Some of the Array arguments is not literal
                return {};
        }

        return std::make_shared<ASTLiteral>(array);
    }

    return {};
}


/** A special hack. If it's [I]LIKE or NOT [I]LIKE expression and the right hand side is a string literal,
  *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
  * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
  * With highlighting we make it clearly obvious.
  *
  * Another case is regexp match. Suppose the user types match(URL, 'www.yandex.ru'). It often means that the user is unaware that . is a metacharacter.
  */
static bool highlightStringLiteralWithMetacharacters(const ASTPtr & node, const IAST::FormatSettings & settings, const char * metacharacters)
{
    if (const auto * literal = node->as<ASTLiteral>())
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


ASTSelectWithUnionQuery * ASTFunction::tryGetQueryArgument() const
{
    if (arguments && arguments->children.size() == 1)
    {
        return arguments->children[0]->as<ASTSelectWithUnionQuery>();
    }
    return nullptr;
}


void ASTFunction::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.expression_list_prepend_whitespace = false;
    FormatStateStacked nested_need_parens = frame;
    FormatStateStacked nested_dont_need_parens = frame;
    nested_need_parens.need_parens = true;
    nested_dont_need_parens.need_parens = false;

    if (auto * query = tryGetQueryArgument())
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        settings.ostr << (settings.hilite ? hilite_function : "") << name << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        query->formatImpl(settings, state, frame_nested);
        settings.ostr << nl_or_nothing << indent_str << ")";
        return;
    }
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
                if (strcasecmp(name.c_str(), func[0]) != 0)
                {
                    continue;
                }

                const auto * literal = arguments->children[0]->as<ASTLiteral>();
                /* A particularly stupid case. If we have a unary minus before
                 * a literal that is a negative number "-(-1)" or "- -1", this
                 * can not be formatted as `--1`, since this will be
                 * interpreted as a comment. Instead, negate the literal
                 * in place. Another possible solution is to use parentheses,
                 * but the old comment said it is impossible, without mentioning
                 * the reason. We should also negate the nonnegative literals,
                 * for symmetry. We print the negated value without parentheses,
                 * because they are not needed around a single literal. Also we
                 * use formatting from FieldVisitorToString, so that the type is
                 * preserved (e.g. -0. is printed with trailing period).
                 */
                if (literal && name == "negate")
                {
                    written = applyVisitor(
                        [&settings](const auto & value)
                        // -INT_MAX is negated to -INT_MAX by the negate()
                        // function, so we can implement this behavior here as
                        // well. Technically it is an UB to perform such negation
                        // w/o a cast to unsigned type.
                        NO_SANITIZE_UNDEFINED
                        {
                            using ValueType = std::decay_t<decltype(value)>;
                            if constexpr (isDecimalField<ValueType>())
                            {
                                // The parser doesn't create decimal literals, but
                                // they can be produced by constant folding or the
                                // fuzzer. Decimals are always signed, so no need
                                // to deduce the result type like we do for ints.
                                const auto int_value = value.getValue().value;
                                settings.ostr << FieldVisitorToString{}(ValueType{
                                    -int_value,
                                    value.getScale()});
                            }
                            else if constexpr (std::is_arithmetic_v<ValueType>)
                            {
                                using ResultType = typename NumberTraits::ResultOfNegate<ValueType>::Type;
                                settings.ostr << FieldVisitorToString{}(
                                    -static_cast<ResultType>(value));
                                return true;
                            }

                            return false;
                        },
                        literal->value);

                    if (written)
                    {
                        break;
                    }
                }

                // We don't need parentheses around a single literal.
                if (!literal && frame.need_parens)
                    settings.ostr << '(';

                settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");

                arguments->formatImpl(settings, state, nested_need_parens);
                written = true;

                if (!literal && frame.need_parens)
                    settings.ostr << ')';

                break;
            }
        }

        /** need_parens - do we need parentheses around the expression with the operator.
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
                "ilike",           " ILIKE ",
                "notLike",         " NOT LIKE ",
                "notILike",        " NOT ILIKE ",
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
                        && (name == "like" || name == "notLike" || name == "ilike" || name == "notILike")
                        && highlightStringLiteralWithMetacharacters(arguments->children[1], settings, "%_");

                    /// Format x IN 1 as x IN (1): put parens around rhs even if there is a single element in set.
                    const auto * second_arg_func = arguments->children[1]->as<ASTFunction>();
                    const auto * second_arg_literal = arguments->children[1]->as<ASTLiteral>();
                    bool extra_parents_around_in_rhs = (name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn")
                        && !second_arg_func
                        && !(second_arg_literal
                             && (second_arg_literal->value.getType() == Field::Types::Tuple
                                || second_arg_literal->value.getType() == Field::Types::Array))
                        && !arguments->children[1]->as<ASTSubquery>();

                    if (extra_parents_around_in_rhs)
                    {
                        settings.ostr << '(';
                        arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                        settings.ostr << ')';
                    }

                    if (!special_hilite && !extra_parents_around_in_rhs)
                        arguments->children[1]->formatImpl(settings, state, nested_need_parens);

                    if (frame.need_parens)
                        settings.ostr << ')';
                    written = true;
                }
            }

            if (!written && 0 == strcmp(name.c_str(), "arrayElement"))
            {
                if (frame.need_parens)
                    settings.ostr << '(';

                arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
                arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
                written = true;

                if (frame.need_parens)
                    settings.ostr << ')';
            }

            if (!written && 0 == strcmp(name.c_str(), "tupleElement"))
            {
                // fuzzer sometimes may inserts tupleElement() created from ASTLiteral:
                //
                //     Function_tupleElement, 0xx
                //     -ExpressionList_, 0xx
                //     --Literal_Int64_255, 0xx
                //     --Literal_Int64_100, 0xx
                //
                // And in this case it will be printed as "255.100", which
                // later will be parsed as float, and formatting will be
                // inconsistent.
                //
                // So instead of printing it as regular tuple,
                // let's print it as ExpressionList instead (i.e. with ", " delimiter).
                bool tuple_arguments_valid = true;
                const auto * lit_left = arguments->children[0]->as<ASTLiteral>();
                const auto * lit_right = arguments->children[1]->as<ASTLiteral>();

                if (lit_left)
                {
                    Field::Types::Which type = lit_left->value.getType();
                    if (type != Field::Types::Tuple && type != Field::Types::Array)
                    {
                        tuple_arguments_valid = false;
                    }
                }

                // It can be printed in a form of 'x.1' only if right hand side
                // is an unsigned integer lineral. We also allow nonnegative
                // signed integer literals, because the fuzzer sometimes inserts
                // them, and we want to have consistent formatting.
                if (tuple_arguments_valid && lit_right)
                {
                    if (isInt64OrUInt64FieldType(lit_right->value.getType())
                        && lit_right->value.get<Int64>() >= 0)
                    {
                        if (frame.need_parens)
                            settings.ostr << '(';

                        arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                        settings.ostr << (settings.hilite ? hilite_operator : "") << "." << (settings.hilite ? hilite_none : "");
                        arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                        written = true;

                        if (frame.need_parens)
                            settings.ostr << ')';
                    }
                }
            }

            if (!written && 0 == strcmp(name.c_str(), "lambda"))
            {
                /// Special case: one-element tuple in lhs of lambda is printed as its element.

                if (frame.need_parens)
                    settings.ostr << '(';

                const auto * first_arg_func = arguments->children[0]->as<ASTFunction>();
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

        if (!written && 0 == strcmp(name.c_str(), "map"))
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << "map(" << (settings.hilite ? hilite_none : "");
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

    if (written)
    {
        return;
    }

    settings.ostr << (settings.hilite ? hilite_function : "") << name;

    if (parameters)
    {
        settings.ostr << '(' << (settings.hilite ? hilite_none : "");
        parameters->formatImpl(settings, state, nested_dont_need_parens);
        settings.ostr << (settings.hilite ? hilite_function : "") << ')';
    }

    if ((arguments && !arguments->children.empty()) || !no_empty_args)
        settings.ostr << '(' << (settings.hilite ? hilite_none : "");

    if (arguments)
    {
        bool special_hilite_regexp = settings.hilite
            && (name == "match" || name == "extract" || name == "extractAll" || name == "replaceRegexpOne"
                || name == "replaceRegexpAll");

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
    }

    if ((arguments && !arguments->children.empty()) || !no_empty_args)
        settings.ostr << (settings.hilite ? hilite_function : "") << ')';

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (!is_window_function)
    {
        return;
    }

    settings.ostr << " OVER ";
    if (!window_name.empty())
    {
        settings.ostr << backQuoteIfNeed(window_name);
    }
    else
    {
        settings.ostr << "(";
        window_definition->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }
}

}
