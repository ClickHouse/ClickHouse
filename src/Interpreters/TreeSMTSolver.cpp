#include <Interpreters/TreeSMTSolver.h>

#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>

#include <math.h>

namespace DB
{

namespace
{
    constexpr double EPS = 1e-10;
}

TreeSMTSolver::TreeSMTSolver(
    STRICTNESS strictness_,
    const NamesAndTypesList & source_columns_)
    : strictness(strictness_)
{
    Poco::Logger::get("TreeSMTSolver").information("RUN");
    try {
        (void)strictness;
        context = std::make_unique<z3::context>();
        for (const auto & column : source_columns_)
        {
            Poco::Logger::get("TreeSMTSolver").information(column.name + " " + column.type->getName());
            /// TODO: add < 0
            if (column.getTypeInStorage()->isNullable())
            {
                const auto uninterpreted_sort = context->uninterpreted_sort(
                    ("sort_" + column.getNameInStorage()).c_str());
                name_to_column.emplace(
                    column.getNameInStorage(),
                    context->constant(column.getNameInStorage().c_str(), uninterpreted_sort));
            }
            else
            {
                switch (column.getTypeInStorage()->getTypeId())
                {
                    case TypeIndex::Float32:
                    case TypeIndex::Float64:
                        name_to_column.emplace(
                            column.getNameInStorage(),
                            context->real_const(column.getNameInStorage().c_str()));
                        break;
                    case TypeIndex::UInt8:
                    case TypeIndex::UInt16:
                    case TypeIndex::UInt32:
                    case TypeIndex::UInt64:
                    case TypeIndex::UInt128:
                    case TypeIndex::UInt256:
                    {
                        const auto & [it, ok]
                            = name_to_column.emplace(column.getNameInStorage(), context->int_const(column.getNameInStorage().c_str()));
                        if (ok)
                            constraints.emplace_back(it->second >= 0);
                        break;
                    }
                    case TypeIndex::Int8:
                    case TypeIndex::Int16:
                    case TypeIndex::Int32:
                    case TypeIndex::Int64:
                    case TypeIndex::Int128:
                    case TypeIndex::Int256:
                        name_to_column.emplace(
                            column.getNameInStorage(),
                            context->int_const(column.getNameInStorage().c_str()));
                        break;
                    default:
                        const auto uninterpreted_sort = context->uninterpreted_sort(
                            ("sort_" + column.getNameInStorage()).c_str());
                        name_to_column.emplace(
                            column.getNameInStorage(),
                            context->constant(column.getNameInStorage().c_str(), uninterpreted_sort));
                        break;
                }
            }
        }
    }
    catch (const z3::exception & e)
    {
        Poco::Logger::get("CONSTRUCTOR").information(e.msg());
        throw Exception(e.msg(), ErrorCodes::LOGICAL_ERROR);
    }
}

const z3::expr & TreeSMTSolver::getOrCreateColumn(const String & name)
{
    if (!name_to_column.contains(name))
    {
        const auto uninterpreted_sort = context->uninterpreted_sort(("sort_" + name).c_str());
        name_to_column.emplace(name, context->constant(name.c_str(), uninterpreted_sort));
    }
    return name_to_column.at(name);
}

const z3::expr & TreeSMTSolver::getColumn(const String & name) const
{
    return name_to_column.at(name);
}

void TreeSMTSolver::addConstraint(const ASTPtr & ast)
{
    Poco::Logger::get("addCONSTR").information("RUN");
    try
    {
        constraints.push_back(transformToLogicExpression(ast));
    }
    catch (const z3::exception & e)
    {
        Poco::Logger::get("addCONSTR").information(e.msg());
        throw Exception(e.msg(), ErrorCodes::LOGICAL_ERROR);
    }
}

bool TreeSMTSolver::alwaysTrue(const ASTPtr & ast)
{
    Poco::Logger::get("alwaysTrue").information("RUN");
    try
    {
        z3::solver solver(*context);
        for (const auto & constraint : constraints)
            solver.add(constraint);
        solver.add(!transformToLogicExpression(ast));
        Poco::Logger::get("alwaysTrue").information(solver.to_smt2());
        Poco::Logger::get("alwaysTrue").information(std::to_string(solver.check() == z3::check_result::unsat));
        return solver.check() == z3::check_result::unsat;
    }
    catch (const z3::exception & e)
    {
        Poco::Logger::get("alwaysTrue").information(e.msg());
        throw Exception(e.msg(), ErrorCodes::LOGICAL_ERROR);
    }
}

bool TreeSMTSolver::alwaysFalse(const ASTPtr & ast)
{
    Poco::Logger::get("alwaysFalse").information("RUN");
    try
    {
        z3::solver solver(*context);
        for (const auto & constraint : constraints)
            solver.add(constraint);
        solver.add(transformToLogicExpression(ast));
        Poco::Logger::get("alwaysFalse").information(solver.to_smt2());
        Poco::Logger::get("alwaysFalse").information(std::to_string(solver.check() == z3::check_result::unsat));
        return solver.check() == z3::check_result::unsat;
    }
    catch (const z3::exception & e)
    {
        Poco::Logger::get("alwaysFalse").information(e.msg());
        throw Exception(e.msg(), ErrorCodes::LOGICAL_ERROR);
    }
}

ASTPtr TreeSMTSolver::minimize(const ASTPtr & ast)
{
    auto opt_expr = transformToLogicExpressionImpl(ast);
    if (!opt_expr || !opt_expr->is_arith())
        return nullptr;
    z3::optimize opt(*context);
    for (const auto & constraint : constraints)
        opt.add(constraint);
    z3::optimize::handle handle = opt.minimize(*opt_expr);
    if (opt.check() != z3::sat)
        return nullptr;
    auto res = opt.lower(handle);

    uint64_t t_ui64;
    if (res.is_numeral_u64(t_ui64))
        return std::make_shared<ASTLiteral>(t_ui64);
    int64_t t_i64;
    if (res.is_numeral_i64(t_i64))
        return std::make_shared<ASTLiteral>(t_i64);
    double t_d;
    if (res.is_numeral(t_d) && !isinf(t_d) && !isnan(t_d))
        return std::make_shared<ASTLiteral>(t_d);
    return nullptr;
}

ASTPtr TreeSMTSolver::maximize(const ASTPtr & ast)
{
    auto opt_expr = transformToLogicExpressionImpl(ast);
    if (!opt_expr || !opt_expr->is_arith())
        return nullptr;
    z3::optimize opt(*context);
    for (const auto & constraint : constraints)
        opt.add(constraint);
    z3::optimize::handle handle = opt.maximize(*opt_expr);
    if (opt.check() != z3::sat)
        return nullptr;
    auto res = opt.upper(handle);

    uint64_t t_ui64;
    if (res.is_numeral_u64(t_ui64))
        return std::make_shared<ASTLiteral>(t_ui64);
    int64_t t_i64;
    if (res.is_numeral_i64(t_i64))
        return std::make_shared<ASTLiteral>(t_i64);
    double t_d;
    if (res.is_numeral(t_d) && !isinf(t_d) && !isnan(t_d))
        return std::make_shared<ASTLiteral>(t_d);
    return nullptr;
}

z3::expr TreeSMTSolver::transformToLogicExpression(const ASTPtr & ast)
{
    z3::expr expr = transformToLogicExpressionImpl(ast).value_or(
        context->bool_const(("unknown_value_" + std::to_string(ast->getTreeHash().first)).c_str()));
    if (expr.is_int())
        return (expr > 0);
    if (!expr.is_bool())
        throw Exception("Expected boolean exception.", ErrorCodes::LOGICAL_ERROR);
    return expr;
}

std::optional<z3::expr> TreeSMTSolver::transformToLogicExpressionImpl(const ASTPtr & ast)
{
    if (const auto * ident = ast->as<ASTIdentifier>(); ident)
    {
        Poco::Logger::get("SMT IDENT").information(ast->dumpTree());
        return getOrCreateColumn(ident->name());
    }
    else if (const auto * lit = ast->as<ASTLiteral>(); lit)
    {
        Poco::Logger::get("SMT LITERAL").information(ast->dumpTree());
        Poco::Logger::get("SMT LITERAL :").information(lit->value.dump());
        switch (lit->value.getType())
        {
            case Field::Types::Which::UInt64:
                return context->int_val(lit->value.get<UInt64>());
            case Field::Types::Which::Int64:
                return context->int_val(lit->value.get<Int64>());
            case Field::Types::Which::Float64:
            {
                z3::expr variable = context->real_const(("LITERAL_" + lit->value.dump() + "_" + std::to_string(random())).c_str());
                if (std::isnan(lit->value.get<double>()))
                {
                    constraints.push_back(context->real_val(0) / context->real_val(0));
                }
                else if (std::isinf(lit->value.get<double>()))
                {
                    if (lit->value.get<double>() > 0)
                        constraints.push_back(context->real_val(1) / context->real_val(0));
                    else
                        constraints.push_back(context->real_val(-1) / context->real_val(0));
                }
                else
                {
                    constraints.push_back(variable < context->real_val(std::to_string(lit->value.get<double>() + EPS).c_str()));
                    constraints.push_back(variable > context->real_val(std::to_string(lit->value.get<double>() - EPS).c_str()));
                }
                return variable;
            }
            default:
                return std::nullopt;
        }
    }
    else if (const auto * func = ast->as<ASTFunction>(); func)
    {
        Poco::Logger::get("SMT FUNCTION").information(ast->dumpTree());
        std::vector<std::optional<z3::expr>> raw_arguments;
        for (const ASTPtr & child : func->arguments->children)
            raw_arguments.push_back(transformToLogicExpressionImpl(child));
        Poco::Logger::get("SMT FUNC").information("args " + std::to_string(raw_arguments.size()));
        Poco::Logger::get("SMT FUNC").information("name " + (func->name));

        if (raw_arguments.size() == 1 && !raw_arguments.front())
            return std::nullopt;

        std::vector<z3::expr> arguments;
        if (std::any_of(
            std::begin(raw_arguments),
            std::end(raw_arguments),
            [](const std::optional<z3::expr> & expr) { return !expr; }))
        {
            auto it = std::find_if(
                std::begin(raw_arguments),
                std::end(raw_arguments),
                [](const std::optional<z3::expr> & expr) { return expr; });
            if (it == std::end(raw_arguments))
                return std::nullopt;

            for (size_t i = 0; i < raw_arguments.size(); ++i)
            {
                if (raw_arguments[i])
                    arguments.push_back(raw_arguments[i].value());
                else
                    arguments.push_back(context->constant(
                        ("UNKNOWN_" + std::to_string(func->arguments->children[i]->getTreeHash().first) + (*it)->get_sort().name().str()).c_str(),
                        (*it)->get_sort())); // TODO int -> real
            }
        }
        else
        {
            for (const auto & expr : raw_arguments)
                arguments.push_back(expr.value());
        }

        if (std::all_of(std::begin(arguments), std::end(arguments),
                        [](const z3::expr & expr) { return expr.is_arith(); }))
        {
            Poco::Logger::get("FIND").information(func->name);
            /// TODO: max,min,abs,sqrt,%
            if (func->name == "plus" && arguments.size() == 2)
            {
                return arguments[0] + arguments[1];
            }
            else if (func->name == "minus" && arguments.size() == 2)
            {
                return arguments[0] - arguments[1];
            }
            else if (func->name == "multiply" && arguments.size() == 2)
            {
                return arguments[0] * arguments[1];
            }
            else if (func->name == "divide" && arguments.size() == 2)
            {
                return arguments[0] / arguments[1];
            }
            else if (func->name == "negate" && arguments.size() == 1)
            {
                return -arguments[0];
            }
            else if (func->name == "less" && arguments.size() == 2)
            {
                return arguments[0] < arguments[1];
            }
            else if (func->name == "lessOrEquals" && arguments.size() == 2)
            {
                return arguments[0] <= arguments[1];
            }
            else if (func->name == "greater" && arguments.size() == 2)
            {
                return arguments[0] > arguments[1];
            }
            else if (func->name == "greaterOrEquals" && arguments.size() == 2)
            {
                return arguments[0] >= arguments[1];
            }
            else if (func->name == "equals" && arguments.size() == 2)
            {
                return arguments[0] == arguments[1];
            }
            else if (func->name == "notEquals" && arguments.size() == 2)
            {
                return arguments[0] != arguments[1];
            }
        }
        if (std::all_of(std::begin(arguments), std::end(arguments),
                             [](const z3::expr & expr) { return expr.is_bool() || expr.is_int(); }))
        {
            for (auto & argument : arguments)
            {
                if (argument.is_int())
                    argument = (argument > 0);
            }
            Poco::Logger::get("FIND").information(func->name);
            if (func->name == "or")
            {
                z3::expr result = arguments[0];
                for (size_t i = 1; i < arguments.size(); ++i)
                    result = result || arguments[i];
                return result;
            }
            else if (func->name == "and")
            {
                z3::expr result = arguments[0];
                for (size_t i = 1; i < arguments.size(); ++i)
                    result = result && arguments[i];
                return result;
            }
            else if (func->name == "xor" && arguments.size() == 2)
            {
                return arguments[0] ^ arguments[1];
            }
            else if (func->name == "not" && arguments.size() == 1)
            {
                Poco::Logger::get("FIND").information("!!!!!!!!!!!!!! NOT");
                return !arguments[0];
            }
            else if (func->name == "equals" && arguments.size() == 2)
            {
                return arguments[0] == arguments[1];
            }
            else if (func->name == "notEquals" && arguments.size() == 2)
            {
                return arguments[0] != arguments[1];
            }
        }
        /// not function but variable with uninterpreted type
        return std::nullopt;
        //return getOrCreateColumn(
        //    "FUNCTION_" + std::to_string(ast->getTreeHash().first) + "_" + std::to_string(ast->getTreeHash().second));
    }
    else
    {
        Poco::Logger::get("BAD AST").information(ast->dumpTree());
        throw Exception("Bad AST", ErrorCodes::LOGICAL_ERROR);
    }
}

}
