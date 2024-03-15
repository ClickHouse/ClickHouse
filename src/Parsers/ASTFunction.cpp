#include <string_view>

#include <Parsers/ASTFunction.h>

#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/KnownObjectNames.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FunctionSecretArgumentsFinderAST.h>
#include <Core/QualifiedTableName.h>

#include <boost/algorithm/string.hpp>


using namespace std::literals;


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_FUNCTION;
}


void ASTFunction::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// These functions contain some unexpected ASTs in arguments (e.g. SETTINGS or even a SELECT query)
    if (name == "view" || name == "viewIfPermitted" || name == "mysql" || name == "postgresql" || name == "mongodb" || name == "s3")
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Table function '{}' cannot be used as an expression", name);

    /// If function can be converted to literal it will be parsed as literal after formatting.
    /// In distributed query it may lead to mismatched column names.
    /// To avoid it we check whether we can convert function to literal.
    if (auto literal = toLiteral())
    {
        literal->appendColumnName(ostr);
        return;
    }

    writeString(name, ostr);

    if (parameters)
    {
        writeChar('(', ostr);
        for (auto * it = parameters->children.begin(); it != parameters->children.end(); ++it)
        {
            if (it != parameters->children.begin())
                writeCString(", ", ostr);

            (*it)->appendColumnName(ostr);
        }
        writeChar(')', ostr);
    }

    writeChar('(', ostr);
    if (arguments)
    {
        for (auto * it = arguments->children.begin(); it != arguments->children.end(); ++it)
        {
            if (it != arguments->children.begin())
                writeCString(", ", ostr);

            (*it)->appendColumnName(ostr);
        }
    }

    writeChar(')', ostr);

    if (nulls_action == NullsAction::RESPECT_NULLS)
        writeCString(" RESPECT NULLS", ostr);
    else if (nulls_action == NullsAction::IGNORE_NULLS)
        writeCString(" IGNORE NULLS", ostr);

    if (is_window_function)
    {
        writeCString(" OVER ", ostr);
        if (!window_name.empty())
        {
            ostr << window_name;
        }
        else
        {
            FormatSettings format_settings{ostr, true /* one_line */};
            FormatState state;
            FormatStateStacked frame;
            writeCString("(", ostr);
            window_definition->formatImpl(format_settings, state, frame);
            writeCString(")", ostr);
        }
    }
}

void ASTFunction::finishFormatWithWindow(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (nulls_action == NullsAction::RESPECT_NULLS)
        settings.ostr << " RESPECT NULLS";
    else if (nulls_action == NullsAction::IGNORE_NULLS)
        settings.ostr << " IGNORE NULLS";

    if (!is_window_function)
        return;

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


void ASTFunction::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(nulls_action);
    if (is_window_function)
    {
        hash_state.update(window_name.size());
        hash_state.update(window_name);
        if (window_definition)
            window_definition->updateTreeHashImpl(hash_state, ignore_aliases);
    }
}

template <typename Container>
static ASTPtr createLiteral(const ASTs & arguments)
{
    Container container;

    for (const auto & arg : arguments)
    {
        if (const auto * literal = arg->as<ASTLiteral>())
        {
            container.push_back(literal->value);
        }
        else if (auto * func = arg->as<ASTFunction>())
        {
            if (auto func_literal = func->toLiteral())
                container.push_back(func_literal->as<ASTLiteral>()->value);
            else
                return {};
        }
        else
            /// Some of the Array or Tuple arguments is not literal
            return {};
    }

    return std::make_shared<ASTLiteral>(container);
}

ASTPtr ASTFunction::toLiteral() const
{
    if (!arguments)
        return {};

    if (name == "array")
        return createLiteral<Array>(arguments->children);

    if (name == "tuple")
        return createLiteral<Tuple>(arguments->children);

    return {};
}


/** A special hack. If it's [I]LIKE or NOT [I]LIKE expression and the right hand side is a string literal,
  *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
  * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
  * With highlighting we make it clearly obvious.
  *
  * Another case is regexp match. Suppose the user types match(URL, 'www.clickhouse.com'). It often means that the user is unaware that . is a metacharacter.
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


static bool formatNamedArgWithHiddenValue(IAST * arg, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame)
{
    const auto * equals_func = arg->as<ASTFunction>();
    if (!equals_func || (equals_func->name != "equals"))
        return false;
    const auto * expr_list = equals_func->arguments->as<ASTExpressionList>();
    if (!expr_list)
        return false;
    const auto & equal_args = expr_list->children;
    if (equal_args.size() != 2)
        return false;

    equal_args[0]->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? IAST::hilite_operator : "") << " = " << (settings.hilite ? IAST::hilite_none : "");
    settings.ostr << "'[HIDDEN]'";

    return true;
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
        settings.ostr << (settings.hilite ? hilite_function : "") << name << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_function : "") << "(" << (settings.hilite ? hilite_none : "");
        settings.ostr << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        query->formatImpl(settings, state, frame_nested);
        settings.ostr << nl_or_nothing << indent_str;
        settings.ostr << (settings.hilite ? hilite_function : "") << ")" << (settings.hilite ? hilite_none : "");
        return;
    }

    /// Should this function to be written as operator?
    bool written = false;
    if (arguments && !parameters && nulls_action == NullsAction::EMPTY)
    {
        /// Unary prefix operators.
        if (arguments->children.size() == 1)
        {
            const char * operators[] =
            {
                "negate",      "-",
                "not",         "NOT ",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (strcasecmp(name.c_str(), func[0]) != 0)
                {
                    continue;
                }

                const auto * literal = arguments->children[0]->as<ASTLiteral>();
                const auto * function = arguments->children[0]->as<ASTFunction>();
                bool is_tuple = literal && literal->value.getType() == Field::Types::Tuple;
                // do not add parentheses for tuple literal, otherwise extra parens will be added `-((3, 7, 3), 1)` -> `-(((3, 7, 3), 1))`
                bool literal_need_parens = literal && !is_tuple;

                // negate always requires parentheses, otherwise -(-1) will be printed as --1
                bool inside_parens = name == "negate" && (literal_need_parens || (function && function->name == "negate"));

                /// We DO need parentheses around a single literal
                /// For example, SELECT (NOT 0) + (NOT 0) cannot be transformed into SELECT NOT 0 + NOT 0, since
                /// this is equal to SELECT NOT (0 + NOT 0)
                bool outside_parens = frame.need_parens && !inside_parens;

                // do not add extra parentheses for functions inside negate, i.e. -(-toUInt64(-(1)))
                if (inside_parens)
                    nested_need_parens.need_parens = false;

                if (outside_parens)
                    settings.ostr << '(';

                settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");

                if (inside_parens)
                    settings.ostr << '(';

                arguments->formatImpl(settings, state, nested_need_parens);
                written = true;

                if (inside_parens)
                    settings.ostr << ')';

                if (outside_parens)
                    settings.ostr << ')';

                break;
            }
        }

        /// Unary postfix operators.
        if (!written && arguments->children.size() == 1)
        {
            const char * operators[] =
            {
                "isNull",          " IS NULL",
                "isNotNull",       " IS NOT NULL",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (strcasecmp(name.c_str(), func[0]) != 0)
                {
                    continue;
                }

                if (frame.need_parens)
                    settings.ostr << '(';
                arguments->formatImpl(settings, state, nested_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
                if (frame.need_parens)
                    settings.ostr << ')';

                written = true;

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
                if (name == std::string_view(func[0]))
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

            if (!written && name == "arrayElement"sv)
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

            if (!written && name == "tupleElement"sv)
            {
                // fuzzer sometimes may insert tupleElement() created from ASTLiteral:
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

            const auto & first_argument = arguments->children[0];
            const ASTIdentifier * first_argument_identifier = first_argument->as<ASTIdentifier>();
            const ASTFunction * first_argument_function = first_argument->as<ASTFunction>();
            bool first_argument_is_tuple = first_argument_function && first_argument_function->name == "tuple";

            /// Only these types of arguments are accepted by the parser of the '->' operator.
            bool acceptable_first_argument_for_lambda_expression = first_argument_identifier || first_argument_is_tuple;

            if (!written && name == "lambda"sv && acceptable_first_argument_for_lambda_expression)
            {
                /// Special case: zero elements tuple in lhs of lambda is printed as ().
                /// Special case: one-element tuple in lhs of lambda is printed as its element.
                /// If lambda function is not the first element in the list, it has to be put in parentheses.
                /// Example: f(x, (y -> z)) should not be printed as f((x, y) -> z).

                if (frame.need_parens || frame.list_element_index > 0)
                    settings.ostr << '(';

                if (first_argument_is_tuple
                    && first_argument_function->arguments
                    && (first_argument_function->arguments->children.size() == 1 || first_argument_function->arguments->children.empty()))
                {
                    if (first_argument_function->arguments->children.size() == 1)
                        first_argument_function->arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                    else
                        settings.ostr << "()";
                }
                else
                    first_argument->formatImpl(settings, state, nested_need_parens);

                settings.ostr << (settings.hilite ? hilite_operator : "") << " -> " << (settings.hilite ? hilite_none : "");
                arguments->children[1]->formatImpl(settings, state, nested_need_parens);
                if (frame.need_parens || frame.list_element_index > 0)
                    settings.ostr << ')';
                written = true;
            }

            if (!written && name == "viewIfPermitted"sv)
            {
                /// viewIfPermitted() needs special formatting: ELSE instead of comma between arguments, and better indents too.
                const auto * nl_or_nothing = settings.one_line ? "" : "\n";
                auto indent0 = settings.one_line ? "" : String(4u * frame.indent, ' ');
                auto indent1 = settings.one_line ? "" : String(4u * (frame.indent + 1), ' ');
                auto indent2 = settings.one_line ? "" : String(4u * (frame.indent + 2), ' ');
                settings.ostr << (settings.hilite ? hilite_function : "") << name << "(" << (settings.hilite ? hilite_none : "") << nl_or_nothing;
                FormatStateStacked frame_nested = frame;
                frame_nested.need_parens = false;
                frame_nested.indent += 2;
                arguments->children[0]->formatImpl(settings, state, frame_nested);
                settings.ostr << nl_or_nothing << indent1 << (settings.hilite ? hilite_keyword : "") << (settings.one_line ? " " : "")
                              << "ELSE " << (settings.hilite ? hilite_none : "") << nl_or_nothing << indent2;
                arguments->children[1]->formatImpl(settings, state, frame_nested);
                settings.ostr << nl_or_nothing << indent0 << ")";
                return;
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
                if (name == std::string_view(func[0]))
                {
                    if (frame.need_parens)
                        settings.ostr << '(';
                    for (size_t i = 0; i < arguments->children.size(); ++i)
                    {
                        if (i != 0)
                            settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
                        if (arguments->children[i]->as<ASTSetQuery>())
                            settings.ostr << "SETTINGS ";
                        arguments->children[i]->formatImpl(settings, state, nested_need_parens);
                    }
                    if (frame.need_parens)
                        settings.ostr << ')';
                    written = true;
                }
            }
        }

        if (!written && name == "array"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
            written = true;
        }

        if (!written && arguments->children.size() >= 2 && name == "tuple"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << ((frame.need_parens && !alias.empty()) ? "tuple" : "") << '('
                          << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ')' << (settings.hilite ? hilite_none : "");
            written = true;
        }

        if (!written && name == "map"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << "map(" << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ')' << (settings.hilite ? hilite_none : "");
            written = true;
        }
    }

    if (written)
    {
        return finishFormatWithWindow(settings, state, frame);
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

        FunctionSecretArgumentsFinder::Result secret_arguments;
        if (!settings.show_secrets)
            secret_arguments = FunctionSecretArgumentsFinderAST(*this).getResult();

        for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << ", ";

            const auto & argument = arguments->children[i];
            if (argument->as<ASTSetQuery>())
                settings.ostr << "SETTINGS ";

            if (!settings.show_secrets)
            {
                if (secret_arguments.start <= i && i < secret_arguments.start + secret_arguments.count)
                {
                    if (secret_arguments.are_named)
                    {
                        assert_cast<const ASTFunction *>(argument.get())->arguments->children[0]->formatImpl(settings, state, nested_dont_need_parens);
                        settings.ostr << (settings.hilite ? hilite_operator : "") << " = " << (settings.hilite ? hilite_none : "");
                    }
                    settings.ostr << "'[HIDDEN]'";
                    if (size <= secret_arguments.start + secret_arguments.count && !secret_arguments.are_named)
                        break; /// All other arguments should also be hidden.
                    continue;
                }

                const ASTFunction * function = argument->as<ASTFunction>();
                if (function && function->arguments && std::count(secret_arguments.nested_maps.begin(), secret_arguments.nested_maps.end(), function->name) != 0)
                {
                    /// headers('foo' = '[HIDDEN]', 'bar' = '[HIDDEN]')
                    settings.ostr << (settings.hilite ? hilite_function : "") << function->name << (settings.hilite ? hilite_none : "") << "(";
                    for (size_t j = 0; j < function->arguments->children.size(); ++j)
                    {
                        if (j != 0)
                            settings.ostr << ", ";
                        auto inner_arg = function->arguments->children[j];
                        if (!formatNamedArgWithHiddenValue(inner_arg.get(), settings, state, nested_dont_need_parens))
                            inner_arg->formatImpl(settings, state, nested_dont_need_parens);
                    }
                    settings.ostr << ")";
                    continue;
                }
            }

            if ((i == 1) && special_hilite_regexp
                && highlightStringLiteralWithMetacharacters(argument, settings, "|()^$.[]?*+{:-"))
            {
                continue;
            }

            nested_dont_need_parens.list_element_index = i;
            argument->formatImpl(settings, state, nested_dont_need_parens);
        }
    }

    if ((arguments && !arguments->children.empty()) || !no_empty_args)
        settings.ostr << (settings.hilite ? hilite_function : "") << ')';

    settings.ostr << (settings.hilite ? hilite_none : "");

    return finishFormatWithWindow(settings, state, frame);
}

bool ASTFunction::hasSecretParts() const
{
    return (FunctionSecretArgumentsFinderAST(*this).getResult().hasSecrets()) || childrenHaveSecretParts();
}

String getFunctionName(const IAST * ast)
{
    String res;
    if (tryGetFunctionNameInto(ast, res))
        return res;
    if (ast)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "{} is not an function", queryToString(*ast));
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "AST node is nullptr");
}

std::optional<String> tryGetFunctionName(const IAST * ast)
{
    String res;
    if (tryGetFunctionNameInto(ast, res))
        return res;
    return {};
}

bool tryGetFunctionNameInto(const IAST * ast, String & name)
{
    if (ast)
    {
        if (const auto * node = ast->as<ASTFunction>())
        {
            name = node->name;
            return true;
        }
    }
    return false;
}

}
