#include <algorithm>
#include <string_view>

#include <Parsers/ASTFunction.h>

#include <boost/algorithm/string/predicate.hpp>

#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/KnownObjectNames.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FunctionSecretArgumentsFinderAST.h>


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
    {
        for (auto it = arguments->children.begin(); it != arguments->children.end(); ++it)
        {
            if (it != arguments->children.begin())
                writeCString(", ", ostr);

            (*it)->appendColumnName(ostr);
        }
    }

    writeChar(')', ostr);

    if (getNullsAction() == NullsAction::RESPECT_NULLS)
        writeCString(" RESPECT NULLS", ostr);
    else if (getNullsAction() == NullsAction::IGNORE_NULLS)
        writeCString(" IGNORE NULLS", ostr);

    if (isWindowFunction())
    {
        writeCString(" OVER ", ostr);
        if (!window_name.empty())
        {
            ostr << window_name;
        }
        else
        {
            FormatSettings format_settings{true /* one_line */};
            FormatState state;
            FormatStateStacked frame;
            writeCString("(", ostr);
            window_definition->format(ostr, format_settings, state, frame);
            writeCString(")", ostr);
        }
    }
}

void ASTFunction::finishFormatWithWindow(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (getNullsAction() == NullsAction::RESPECT_NULLS)
        ostr << " RESPECT NULLS";
    else if (getNullsAction() == NullsAction::IGNORE_NULLS)
        ostr << " IGNORE NULLS";

    if (!isWindowFunction())
        return;

    ostr << " OVER ";
    if (!window_name.empty())
    {
        ostr << backQuoteIfNeed(window_name);
    }
    else
    {
        ostr << "(";
        window_definition->format(ostr, settings, state, frame);
        ostr << ")";
    }
}

/** Get the text that identifies this element. */
String ASTFunction::getID(char delim) const
{
    return "Function" + (delim + name);
}

ASTPtr ASTFunction::clone() const
{
    auto res = make_intrusive<ASTFunction>(*this);
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

    hash_state.update(getNullsAction());
    if (isWindowFunction())
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

    return make_intrusive<ASTLiteral>(container);
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


ASTSelectWithUnionQuery * ASTFunction::tryGetQueryArgument() const
{
    if (arguments && arguments->children.size() == 1)
    {
        return arguments->children[0]->as<ASTSelectWithUnionQuery>();
    }
    return nullptr;
}


static bool formatNamedArgWithHiddenValue(IAST * arg, WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame)
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

    equal_args[0]->format(ostr, settings, state, frame);
    ostr << " = ";
    ostr << "'[HIDDEN]'";

    return true;
}

/// Only some types of arguments are accepted by the parser of the '->' operator.
static bool isAcceptableArgumentsForLambdaExpression(const ASTs & arguments)
{
    if (arguments.size() == 2)
    {
        const auto & first_argument = arguments[0];
        if (first_argument->as<ASTIdentifier>())
            return true;
        const ASTFunction * first_argument_function = first_argument->as<ASTFunction>();
        if (first_argument_function && (first_argument_function->name == "tuple") && first_argument_function->arguments)
        {
            const auto & tuple_args = first_argument_function->arguments->children;
            auto all_tuple_arguments_are_identifiers
                = std::all_of(tuple_args.begin(), tuple_args.end(), [](const ASTPtr & x) { return x->as<ASTIdentifier>(); });
            if (all_tuple_arguments_are_identifiers)
                return true;
        }
    }
    return false;
}

namespace
{

struct FunctionOperatorMapping
{
    std::string_view function_name;
    std::string_view operator_name;
};

}

void ASTFunction::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.expression_list_prepend_whitespace = false;
    auto kind = getKind();
    if (kind == Kind::CODEC || kind == Kind::STATISTICS || kind == Kind::BACKUP_NAME)
        frame.allow_operators = false;
    FormatStateStacked nested_need_parens = frame;
    FormatStateStacked nested_dont_need_parens = frame;
    nested_need_parens.need_parens = true;
    nested_dont_need_parens.need_parens = false;

    if (auto * query = tryGetQueryArgument())
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        if (!name.empty())
            ostr << backQuoteIfNeed(name);
        ostr << "(";
        ostr << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        query->format(ostr, settings, state, frame_nested);
        ostr << nl_or_nothing << indent_str;
        ostr << ")";
        return;
    }

    if (arguments && !parameters && arguments->children.size() == 2 && name == "viewIfPermitted"sv)
    {
        /// viewIfPermitted() needs special formatting: ELSE instead of comma between arguments, and better indents too.
        const auto * nl_or_nothing = settings.one_line ? "" : "\n";
        auto indent0 = settings.one_line ? "" : String(4u * frame.indent, ' ');
        auto indent1 = settings.one_line ? "" : String(4u * (frame.indent + 1), ' ');
        auto indent2 = settings.one_line ? "" : String(4u * (frame.indent + 2), ' ');
        ostr << name << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        frame_nested.indent += 2;
        arguments->children[0]->format(ostr, settings, state, frame_nested);
        ostr << nl_or_nothing << indent1 << (settings.one_line ? " " : "")
             << "ELSE " << nl_or_nothing << indent2;
        arguments->children[1]->format(ostr, settings, state, frame_nested);
        ostr << nl_or_nothing << indent0 << ")";
        return;
    }

    /// Should this function to be written as operator?
    bool written = false;
    if (isOperator() && arguments && !parameters && frame.allow_operators && getNullsAction() == NullsAction::EMPTY)
    {
        /// Unary prefix operators.
        if (arguments->children.size() == 1)
        {
            static constexpr std::array<FunctionOperatorMapping, 2> operators = {{
                {"negate", "-"},
                {"not", "NOT "},
            }};

            if (auto it = std::ranges::find_if(operators, [&](const auto & op) { return boost::iequals(name, op.function_name); });
                it != operators.end())
            {
                const auto & func_symbol = it->operator_name;

                const auto * literal = arguments->children[0]->as<ASTLiteral>();
                const auto * function = arguments->children[0]->as<ASTFunction>();
                const auto * subquery = arguments->children[0]->as<ASTSubquery>();
                bool is_tuple = (literal && literal->value.getType() == Field::Types::Tuple)
                             || (function && function->name == "tuple" && function->arguments && function->arguments->children.size() > 1);
                bool is_array = (literal && literal->value.getType() == Field::Types::Array)
                             || (function && function->name == "array");
                bool has_alias = !arguments->children[0]->tryGetAlias().empty();

                /// Do not add parentheses for tuple and array literal, otherwise extra parens will be added `-((3, 7, 3), 1)` -> `-(((3, 7, 3), 1))`, `-[1]` -> `-([1])`
                bool literal_need_parens = literal && !is_tuple && !is_array;

                /// Negate always requires parentheses, otherwise -(-1) will be printed as --1
                /// Also extra parentheses are needed for subqueries and tuple, because NOT can be parsed as a function:
                /// not(SELECT 1) cannot be parsed, while not((SELECT 1)) can.
                /// not((1, 2, 3)) is a function of one argument, while not(1, 2, 3) is a function of three arguments.
                /// Note: If the arg to negate/not/- has an alias, we never need the inside parens
                bool inside_parens = !has_alias
                    && ((name == "negate" && (literal_need_parens || (function && function->name == "negate")))
                        || (subquery && name == "not") || (is_tuple && name == "not"));

                /// We DO need parentheses around a single literal
                /// For example, SELECT (NOT 0) + (NOT 0) cannot be transformed into SELECT NOT 0 + NOT 0, since
                /// this is equal to SELECT NOT (0 + NOT 0)
                bool outside_parens = frame.need_parens && (!frame.allow_moving_operators_before_parens || !inside_parens);

                /// Do not add extra parentheses for functions inside negate, i.e. -(-toUInt64(-(1)))
                if (inside_parens)
                    nested_need_parens.need_parens = false;

                if (outside_parens)
                    ostr << '(';

                ostr << func_symbol;

                if (inside_parens)
                    ostr << '(';

                arguments->format(ostr, settings, state, nested_need_parens);
                written = true;

                if (inside_parens)
                    ostr << ')';

                if (outside_parens)
                    ostr << ')';
            }
        }

        /// Unary postfix operators.
        if (!written && arguments->children.size() == 1)
        {
            static constexpr std::array<FunctionOperatorMapping, 2> operators = {{
                {"isNull", " IS NULL"},
                {"isNotNull", " IS NOT NULL"},
            }};

            if (auto it = std::ranges::find_if(operators, [&](const auto & op) { return boost::iequals(name, op.function_name); });
                it != operators.end())
            {
                if (frame.need_parens)
                    ostr << '(';
                arguments->format(ostr, settings, state, nested_need_parens);
                ostr << it->operator_name;
                if (frame.need_parens)
                    ostr << ')';

                written = true;
            }
        }

        /** need_parens - do we need parentheses around the expression with the operator.
          * They are needed only if this expression is included in another expression with the operator.
          */

        if (!written && arguments->children.size() == 2)
        {
            static constexpr std::array<FunctionOperatorMapping, 21> operators =
            {{
                {"multiply",          " * "},
                {"divide",            " / "},
                {"modulo",            " % "},
                {"plus",              " + "},
                {"minus",             " - "},
                {"notEquals",         " != "},
                {"lessOrEquals",      " <= "},
                {"greaterOrEquals",   " >= "},
                {"less",              " < "},
                {"greater",           " > "},
                {"equals",            " = "},
                {"isNotDistinctFrom", " <=> "},
                {"isDistinctFrom",    " IS DISTINCT FROM "},
                {"like",              " LIKE "},
                {"ilike",             " ILIKE "},
                {"notLike",           " NOT LIKE "},
                {"notILike",          " NOT ILIKE "},
                {"in",                " IN "},
                {"notIn",             " NOT IN "},
                {"globalIn",          " GLOBAL IN "},
                {"globalNotIn",       " GLOBAL NOT IN "}
            }};

            if (auto it = std::ranges::find(operators, name, &FunctionOperatorMapping::function_name); it != operators.end())
            {
                /// IN operators need extra parentheses to avoid parsing ambiguity when used as function arguments.
                /// The parser cannot handle IN inside multi-argument function calls without parentheses.
                /// Example: position(1 IN (SELECT 1), 2) must be formatted as position((1 IN (SELECT 1)), 2)
                bool is_in_operator = (name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn");
                bool in_function_args = frame.current_function != nullptr;
                bool need_parens_around_in = frame.need_parens || (is_in_operator && in_function_args);
                if (need_parens_around_in)
                    ostr << '(';
                arguments->children[0]->format(ostr, settings, state, nested_need_parens);
                ostr << it->operator_name;

                /// Format x IN 1 as x IN (1): put parens around rhs even if there is a single element in set.
                const auto * second_arg_func = arguments->children[1]->as<ASTFunction>();
                const auto * second_arg_literal = arguments->children[1]->as<ASTLiteral>();
                bool is_literal_tuple_or_array = second_arg_literal
                    && (second_arg_literal->value.getType() == Field::Types::Tuple
                        || second_arg_literal->value.getType() == Field::Types::Array);

                /** Conditions for extra parens:
                 *  1. Is IN operator
                 *  2. 2nd arg is not subquery, function, or literal tuple or array
                 *  3. If the 2nd argument has alias, we ignore condition 2 and add extra parens
                 *
                 *  Condition 3 is needed to avoid inconsistency in format-parse-format debug check in executeQuery.cpp
                 */
                bool extra_parents_around_in_rhs = is_in_operator
                    && ((!arguments->children[1]->as<ASTSubquery>() && !second_arg_func && !is_literal_tuple_or_array)
                        || !arguments->children[1]->tryGetAlias().empty());

                if (extra_parents_around_in_rhs)
                {
                    ostr << '(';
                    arguments->children[1]->format(ostr, settings, state, nested_dont_need_parens);
                    ostr << ')';
                }

                if (!extra_parents_around_in_rhs)
                    arguments->children[1]->format(ostr, settings, state, nested_need_parens);

                if (need_parens_around_in)
                    ostr << ')';
                written = true;
            }

            if (!written && name == "arrayElement"sv)
            {
                if (frame.need_parens)
                    ostr << '(';

                /// Don't allow moving operators like '-' before parents,
                /// otherwise (-(42))[3] will be formatted as -(42)[3] that will be parsed as -(42[3]);
                nested_need_parens.allow_moving_operators_before_parens = false;
                arguments->children[0]->format(ostr, settings, state, nested_need_parens);
                ostr << '[';
                arguments->children[1]->format(ostr, settings, state, nested_dont_need_parens);
                ostr << ']';
                written = true;

                if (frame.need_parens)
                    ostr << ')';
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

                if (arguments->children[0]->as<ASTAsterisk>())
                    tuple_arguments_valid = false;

                if (lit_left)
                {
                    Field::Types::Which type = lit_left->value.getType();
                    if (type != Field::Types::Tuple && type != Field::Types::Array)
                    {
                        tuple_arguments_valid = false;
                    }
                }

                /// It can be printed in a form of 'x.1' only if right hand side
                /// is an unsigned integer lineral. We also allow nonnegative
                /// signed integer literals, because the fuzzer sometimes inserts
                /// them, and we want to have consistent formatting.
                if (tuple_arguments_valid && lit_right)
                {
                    if (isInt64OrUInt64FieldType(lit_right->value.getType())
                        && lit_right->value.safeGet<Int64>() >= 0)
                    {
                        if (frame.need_parens)
                            ostr << '(';

                        /// Little hack: Expression like this: (tab.*).1 (tab contains single tuple column)
                        /// causes inconsistent formatting because it is formatted as tab.*.1 which is invalid.
                        /// So when child 0 has more than one element, we surround it with parens.
                        /// Exception: array and tuple functions format with their own brackets ([...] and (...)),
                        /// which are already unambiguous with .N syntax. Adding extra parens around them
                        /// would cause inconsistent formatting when re-parsed, because the parser's fast path
                        /// creates ASTLiteral (size=1, no parens) while ASTFunction has size>1.
                        const auto * left_func = arguments->children[0]->as<ASTFunction>();
                        bool left_needs_parens = arguments->children[0]->size() > 1
                            && !(left_func && (left_func->name == "array" || left_func->name == "tuple"));

                        if (left_needs_parens)
                        {
                            nested_need_parens.need_parens = false; /// Don't want duplicate parens
                            ostr << '(';
                        }

                        /// Don't allow moving operators like '-' before parents,
                        /// otherwise (-(42)).1 will be formatted as -(42).1 that will be parsed as -((42).1)
                        nested_need_parens.allow_moving_operators_before_parens = false;
                        arguments->children[0]->format(ostr, settings, state, nested_need_parens);

                        if (left_needs_parens)
                            ostr << ')';

                        ostr << ".";
                        arguments->children[1]->format(ostr, settings, state, nested_dont_need_parens);
                        written = true;

                        if (frame.need_parens)
                            ostr << ')';
                    }
                }
            }

            /// Only some types of arguments are accepted by the parser of the '->' operator.
            if (!written && name == "lambda"sv && isAcceptableArgumentsForLambdaExpression(arguments->children))
            {
                const auto & first_argument = arguments->children[0];
                const ASTFunction * first_argument_function = first_argument->as<ASTFunction>();
                bool first_argument_is_tuple = first_argument_function && first_argument_function->name == "tuple";

                /// Special case: zero elements tuple in lhs of lambda is printed as ().
                /// Special case: one-element tuple in lhs of lambda is printed as its element.
                /// If lambda function is not the first element in the list, it has to be put in parentheses.
                /// Example: f(x, (y -> z)) should not be printed as f((x, y) -> z).

                if (frame.need_parens || frame.list_element_index > 0)
                    ostr << '(';

                if (first_argument_is_tuple
                    && first_argument_function->arguments
                    && (first_argument_function->arguments->children.size() == 1 || first_argument_function->arguments->children.empty()))
                {
                    if (first_argument_function->arguments->children.size() == 1)
                        first_argument_function->arguments->children[0]->format(ostr, settings, state, nested_need_parens);
                    else
                        ostr << "()";
                }
                else
                    first_argument->format(ostr, settings, state, nested_need_parens);

                ostr << " -> ";
                arguments->children[1]->format(ostr, settings, state, nested_need_parens);
                if (frame.need_parens || frame.list_element_index > 0)
                    ostr << ')';
                written = true;
            }
        }

        if (!written && arguments->children.size() >= 2)
        {
            constexpr std::array<FunctionOperatorMapping, 2> operators
            {{
                {"and", " AND "},
                {"or", " OR "}
            }};

            if (auto it = std::ranges::find(operators, name, &FunctionOperatorMapping::function_name); it != operators.end())
            {
                if (frame.need_parens)
                    ostr << '(';
                for (size_t i = 0; i < arguments->children.size(); ++i)
                {
                    if (i != 0)
                        ostr << it->operator_name;
                    if (arguments->children[i]->as<ASTSetQuery>())
                        ostr << "SETTINGS ";
                    arguments->children[i]->format(ostr, settings, state, nested_need_parens);
                }
                if (frame.need_parens)
                    ostr << ')';
                written = true;
            }
        }

        if (!written && name == "array"sv)
        {
            ostr << '[';
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    ostr << "SETTINGS ";
                nested_dont_need_parens.list_element_index = i;
                arguments->children[i]->format(ostr, settings, state, nested_dont_need_parens);
            }
            ostr << ']';
            written = true;
        }

        if (!written && arguments->children.size() >= 2 && name == "tuple"sv && !(frame.need_parens && !alias.empty()))
        {
            ostr << '(';

            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    ostr << "SETTINGS ";
                nested_dont_need_parens.list_element_index = i;
                arguments->children[i]->format(ostr, settings, state, nested_dont_need_parens);
            }
            ostr << ')';
            written = true;
        }

        if (!written && name == "map"sv)
        {
            ostr << "map(";
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    ostr << "SETTINGS ";
                nested_dont_need_parens.list_element_index = i;
                arguments->children[i]->format(ostr, settings, state, nested_dont_need_parens);
            }
            ostr << ')';
            written = true;
        }
    }

    if (written)
    {
        finishFormatWithWindow(ostr, settings, state, frame);
        return;
    }

    /// Empty names are used rarely, to format queries with an extra pair of parentheses for external databases.
    if (!name.empty())
        ostr << backQuoteIfNeed(name);

    if (parameters)
    {
        ostr << '(';
        parameters->format(ostr, settings, state, nested_dont_need_parens);
        ostr << ')';
    }

    /// If the function has a NULLS modifier (IGNORE NULLS / RESPECT NULLS), we must always print
    /// parentheses, otherwise the modifier cannot be parsed back (e.g. `count IGNORE NULLS` is not parseable).
    bool has_nulls_action = getNullsAction() != NullsAction::EMPTY;
    bool need_parens = (arguments && !arguments->children.empty()) || !noEmptyArgs() || has_nulls_action;

    if (need_parens)
        ostr << '(';

    if (arguments)
    {
        FunctionSecretArgumentsFinder::Result secret_arguments;
        if (!settings.show_secrets)
            secret_arguments = FunctionSecretArgumentsFinderAST(*this).getResult();

        for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
        {
            if (i != 0)
                ostr << ", ";

            const auto & argument = arguments->children[i];
            if (argument->as<ASTSetQuery>())
                ostr << "SETTINGS ";

            if (!settings.show_secrets)
            {
                if (secret_arguments.start <= i && i < secret_arguments.start + secret_arguments.count)
                {
                    if (secret_arguments.are_named)
                    {
                        if (const auto * func_ast = typeid_cast<const ASTFunction *>(argument.get()))
                            func_ast->arguments->children[0]->format(ostr, settings, state, nested_dont_need_parens);
                        else
                            argument->format(ostr, settings, state, nested_dont_need_parens);
                        ostr << " = ";
                    }
                    if (!secret_arguments.replacement.empty())
                    {
                        if (secret_arguments.quote_replacement)
                        {
                            ostr << "'" << secret_arguments.replacement << "'";
                        }
                        else
                        {
                            ostr << secret_arguments.replacement;
                        }
                    }
                    else
                    {
                        ostr << "'[HIDDEN]'";
                    }
                    if (size <= secret_arguments.start + secret_arguments.count && !secret_arguments.are_named)
                        break; /// All other arguments should also be hidden.
                    continue;
                }

                const ASTFunction * function = argument->as<ASTFunction>();
                if (function && function->arguments && std::count(secret_arguments.nested_maps.begin(), secret_arguments.nested_maps.end(), function->name) != 0)
                {
                    /// headers('foo' = '[HIDDEN]', 'bar' = '[HIDDEN]')
                    ostr << function->name << "(";
                    for (size_t j = 0; j < function->arguments->children.size(); ++j)
                    {
                        if (j != 0)
                            ostr << ", ";
                        auto inner_arg = function->arguments->children[j];
                        if (!formatNamedArgWithHiddenValue(inner_arg.get(), ostr, settings, state, nested_dont_need_parens))
                            inner_arg->format(ostr, settings, state, nested_dont_need_parens);
                    }
                    ostr << ")";
                    continue;
                }
            }

            nested_dont_need_parens.list_element_index = i;
            /// Mark that we're formatting an argument of this function (needed for IN operator parentheses)
            if (arguments->children.size() > 1)
                nested_dont_need_parens.current_function = this;
            argument->format(ostr, settings, state, nested_dont_need_parens);
        }
    }

    if (need_parens)
        ostr << ')';

    finishFormatWithWindow(ostr, settings, state, frame);
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
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "{} is not an function", ast->formatForErrorMessage());
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

bool isASTLambdaFunction(const ASTFunction & function)
{
    if (function.name == "lambda" && function.arguments && function.arguments->children.size() == 2)
    {
        const auto * lambda_args_tuple = function.arguments->children.at(0)->as<ASTFunction>();
        return lambda_args_tuple && lambda_args_tuple->name == "tuple";
    }

    return false;
}

}
