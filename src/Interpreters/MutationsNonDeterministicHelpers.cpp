#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/MutationCommands.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/NaNUtils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/TypeTree.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/misc.h>
#include <Functions/FunctionFactory.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <map>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool mutations_execute_nondeterministic_on_initiator;
    extern const SettingsBool mutations_execute_subqueries_on_initiator;
    extern const SettingsUInt64 mutations_max_literal_size_to_replace;
    extern const SettingsBool use_concurrency_control;
}

namespace
{

/// Helps to detect situations, where non-deterministic functions may be used in mutations.
class FirstNonDeterministicFunctionMatcher
{
public:
    struct Data
    {
        ContextPtr context;
        FirstNonDeterministicFunctionResult result;
    };

    static bool needChildVisit(const ASTPtr & /*node*/, const ASTPtr & /*child*/)
    {
        return true;
    }

    static void visit(const ASTPtr & node, Data & data)
    {
        if (data.result.nondeterministic_function_name || data.result.subquery)
            return;

        if (node->as<ASTSelectQuery>())
        {
            /// We cannot determine if subquery is deterministic or not,
            /// so we do not allow to use subqueries in mutation without allow_nondeterministic_mutations=1
            data.result.subquery = true;
        }
        else if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
        {
            /// Property of being deterministic for lambda expression is completely determined
            /// by the contents of its definition, so we just proceed to it.
            if (function->name != "lambda")
            {
                /// NOTE It may be an aggregate function, so get(...) may throw.
                /// However, an aggregate function can be used only in subquery and we do not go into subquery.
                const auto func = FunctionFactory::instance().get(function->name, data.context);
                if (!func->isDeterministic())
                    data.result.nondeterministic_function_name = func->getName();
            }
        }
    }
};

using FirstNonDeterministicFunctionFinder = InDepthNodeVisitor<FirstNonDeterministicFunctionMatcher, true>;
using FirstNonDeterministicFunctionData = FirstNonDeterministicFunctionMatcher::Data;

/// Executes and replaces with literals
/// non-deterministic functions in query.
/// Similar to ExecuteScalarSubqueriesVisitor.
class ExecuteNonDeterministicConstFunctionsMatcher
{
public:

    struct Data
    {
        ContextPtr context;
        std::optional<size_t> max_literal_size;
    };

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & /*child*/)
    {
        /// Do not visit subqueries because they are executed separately.
        return !ast->as<ASTSelectQuery>();
    }

    static void visit(ASTPtr & ast, const Data & data)
    {
        if (auto * function = ast->as<ASTFunction>())
            visit(*function, ast, data);
    }

    static void visit(ASTFunction & function, ASTPtr & ast, const Data & data)
    {
        if (!FunctionFactory::instance().has(function.name))
            return;

        /// It makes sense to execute functions which are deterministic
        /// in scope of query because they are usually constant expressions.
        auto builder = FunctionFactory::instance().get(function.name, data.context);
        if (builder->isDeterministic() || !builder->isDeterministicInScopeOfQuery())
            return;

        Field field;
        DataTypePtr type;

        try
        {
            std::tie(field, type) = evaluateConstantExpression(ast, data.context);
        }
        catch (...)
        {
            /// An exception can be thrown if the expression is not constant.
            /// It's ok in that context and we just do nothing in that case.
            /// It's bad pattern but it's quite hard to implement it in another way.
            return;
        }

        auto column = type->createColumn();
        column->insert(field);

        Block scalar{{std::move(column), type, "_constant"}};
        if (worthConvertingScalarToLiteral(scalar, data.max_literal_size))
        {
            auto literal = make_intrusive<ASTLiteral>(std::move(field));
            ast = addTypeConversionToAST(std::move(literal), type->getName());
        }
    }
};

using ExecuteNonDeterministicConstFunctionsVisitor = InDepthNodeVisitor<ExecuteNonDeterministicConstFunctionsMatcher, true>;

/// Executes the subqueries of `IN` and `exists` and replaces them with their results.
/// The subquery of `IN` becomes a subquery over the literal of its result: a set built from literals would
/// convert them to the type of the left argument, while a set built from a subquery keeps the subquery's types.
class ExecuteSetSubqueriesMatcher
{
public:
    struct Data
    {
        ContextPtr context;
        size_t max_literal_size;
        /// The aliases of the command outside its subqueries, which a subquery cannot resolve on its own.
        NameSet aliases;
        /// The replacement of each subquery executed so far, nullptr if the subquery is kept.
        /// All occurrences of a subquery in a command share one set, so they must see one result.
        std::map<std::pair<bool, IASTHash>, ASTPtr> results;
    };

    static void collectAliases(const IAST & ast, NameSet & aliases)
    {
        if (auto alias = ast.tryGetAlias(); !alias.empty())
            aliases.insert(std::move(alias));
        if (!ast.as<ASTSubquery>())
            for (const auto & child : ast.children)
                collectAliases(*child, aliases);
    }

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/)
    {
        return !node->as<ASTSubquery>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        auto * function = ast->as<ASTFunction>();
        if (!function || !function->arguments)
            return;

        if (checkFunctionIsInOrGlobalInOperator(*function))
        {
            auto & argument = function->arguments->children[1];
            if (!argument->as<ASTSubquery>())
                return;

            if (auto result = getResult(argument, /*is_exists=*/ false, data))
            {
                result->setAlias(argument->as<ASTSubquery &>().alias);
                argument = std::move(result);
            }
        }
        else if (function->name == "exists" && function->arguments->children.size() == 1
            && function->arguments->children[0]->as<ASTSubquery>())
        {
            if (auto result = getResult(function->arguments->children[0], /*is_exists=*/ true, data))
            {
                result->setAlias(function->alias);
                ast = std::move(result);
            }
        }
    }

private:
    /// `ColumnArray::get` refuses to build a larger array.
    static constexpr size_t max_array_size_as_field = 1'000'000;

    static bool usesAlias(const IAST & ast, const NameSet & aliases)
    {
        if (const auto * identifier = ast.as<ASTIdentifier>())
            return aliases.contains(identifier->name_parts.front());
        return std::ranges::any_of(ast.children, [&](const auto & child) { return usesAlias(*child, aliases); });
    }

    template <typename T>
    static bool hasNaN(const IColumn & column)
    {
        const auto * floats = typeid_cast<const ColumnVector<T> *>(&column);
        return floats && std::ranges::any_of(floats->getData(), [](T x) { return isNaN(x); });
    }

    static bool hasLongDecimal(const IColumn & column)
    {
        const auto * decimals = typeid_cast<const ColumnDecimal<Decimal256> *>(&column);
        const auto bound = DecimalUtils::scaleMultiplier<Int256>(DecimalUtils::max_precision<Decimal256>);
        return decimals && std::ranges::any_of(decimals->getData(), [&](const Decimal256 & x) { return x.value <= -bound || x.value >= bound; });
    }

    static ASTPtr getResult(const ASTPtr & subquery, bool is_exists, Data & data)
    {
        if (usesAlias(*subquery, data.aliases))
            return nullptr;

        auto key = std::make_pair(is_exists, subquery->getTreeHash(/*ignore_aliases=*/ false));
        auto it = data.results.find(key);
        if (it == data.results.end())
            it = data.results.emplace(key, is_exists ? executeExists(subquery, data) : executeIn(subquery, data)).first;
        return it->second ? it->second->clone() : nullptr;
    }

    static ASTPtr makeSubquery(ASTPtr expression, ASTPtr tables = nullptr, ASTPtr limit = nullptr)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(std::move(expression));

        auto select = make_intrusive<ASTSelectQuery>();
        select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
        select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit));

        auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union->list_of_selects = make_intrusive<ASTExpressionList>();
        select_with_union->list_of_selects->children.push_back(std::move(select));
        select_with_union->children.push_back(select_with_union->list_of_selects);

        return make_intrusive<ASTSubquery>(std::move(select_with_union));
    }

    /// Calls `on_block` for every non-empty block of the result until it returns false.
    template <typename OnBlock>
    static void execute(InterpreterSelectQueryAnalyzer & interpreter, const ContextPtr & context, OnBlock && on_block)
    {
        auto io = interpreter.execute();

        PullingAsyncPipelineExecutor executor(io.pipeline);
        io.pipeline.setProgressCallback(context->getProgressCallback());
        io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);
        if (auto cancel_cb = context->hasQueryContext() ? context->getQueryContext()->getInteractiveCancelCallback() : nullptr)
            executor.setCancelCallback(std::move(cancel_cb), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));

        Block block;
        while (executor.pull(block))
            if (block.rows() && !on_block(block))
                return;
    }

    /// Executes `SELECT 1 FROM (subquery) LIMIT 1`, as the analyzer does for `exists`.
    static ASTPtr executeExists(const ASTPtr & subquery, const Data & data)
    {
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = subquery->clone();
        table_expression->children.push_back(table_expression->subquery);

        auto tables_element = make_intrusive<ASTTablesInSelectQueryElement>();
        tables_element->table_expression = table_expression;
        tables_element->children.push_back(tables_element->table_expression);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(tables_element));

        auto exists_subquery = makeSubquery(
            make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(1))), std::move(tables), make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(1))));

        bool has_rows = false;
        auto interpreter = interpretSubqueryWithAnalyzer(exists_subquery, data.context, /*subquery_depth=*/ 1, {});
        execute(*interpreter, data.context, [&](const Block &) { has_rows = true; return false; });

        return addTypeConversionToAST(make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(has_rows))), "UInt8");
    }

    static ASTPtr executeIn(const ASTPtr & subquery, const Data & data)
    {
        auto interpreter = interpretSubqueryWithAnalyzer(subquery, data.context, /*subquery_depth=*/ 1, {});
        DataTypes types = interpreter->getSampleBlock()->getDataTypes();

        /// Values of these types do not survive the round trip through a literal.
        for (const auto & type : types)
            if (anyInTypeTree(*type, [](const IDataType & node) { return isVariant(node) || isDynamic(node) || isObject(node) || isQBit(node); }))
                return nullptr;

        MutableColumns columns;
        for (const auto & type : types)
            columns.push_back(type->createColumn());

        size_t rows = 0;
        bool fits = true;
        execute(*interpreter, data.context, [&](const Block & block)
        {
            auto materialized = materializeBlock(block);
            size_t bytes = 0;
            for (size_t i = 0; i < columns.size(); ++i)
            {
                columns[i]->insertRangeFrom(*materialized.getByPosition(i).column, 0, materialized.rows());
                bytes += columns[i]->byteSize();
            }
            rows += materialized.rows();

            /// Every element takes at least one character of the literal, even if it takes no bytes in the column.
            fits = rows <= data.max_literal_size && rows <= max_array_size_as_field && bytes <= data.max_literal_size;
            return fits;
        });

        if (!fits)
            return nullptr;

        /// `ColumnArray::get` refuses nested arrays of more than `max_array_size_as_field` elements too. The text of a NaN
        /// loses its payload, which `IN` compares, and no decimal type reads back a value of more than 76 digits.
        bool round_trips = true;
        auto check_values = [&](const IColumn & column)
        {
            if (const auto * nested = typeid_cast<const ColumnArray *>(&column))
            {
                for (size_t i = 0; i < nested->size() && round_trips; ++i)
                    round_trips = nested->getSize(i) <= max_array_size_as_field;
            }
            else if (hasNaN<BFloat16>(column) || hasNaN<Float32>(column) || hasNaN<Float64>(column) || hasLongDecimal(column))
                round_trips = false;
        };
        for (const auto & column : columns)
        {
            /// A shared dictionary of `LowCardinality` is not a subcolumn.
            auto values = recursiveRemoveLowCardinality(column->getPtr());
            check_values(*values);
            values->forEachSubcolumnRecursively(check_values);
        }

        if (!round_trips)
            return nullptr;

        size_t num_columns = columns.size();
        ColumnPtr elements;
        DataTypePtr element_type;
        if (num_columns == 1)
        {
            elements = std::move(columns[0]);
            element_type = types[0];
        }
        else
        {
            elements = ColumnTuple::create(std::move(columns));
            element_type = std::make_shared<DataTypeTuple>(types);
        }

        auto array_type = std::make_shared<DataTypeArray>(element_type);
        Block array{{ColumnArray::create(elements, ColumnArray::ColumnOffsets::create(1, rows)), array_type, "_set"}};
        if (!worthConvertingScalarToLiteral(array, data.max_literal_size))
            return nullptr;

        /// The text of a `Decimal`, `DateTime64` or `Time64` value is not always read back by its own type,
        /// while `Decimal(76, S)` reads back any value of up to 76 digits.
        auto literal_type = rewriteTypeTree(array_type, [](const DataTypePtr & node) -> DataTypePtr
        {
            if (auto scale = tryGetDecimalScale(*node))
                return std::make_shared<DataTypeDecimal256>(DecimalUtils::max_precision<Decimal256>, *scale);
            return node;
        }, CustomizationPolicy::Keep);

        auto literal_column = castColumn(array.getByPosition(0), literal_type);
        ASTPtr literal = addTypeConversionToAST(make_intrusive<ASTLiteral>((*literal_column)[0]), literal_type->getName());
        if (!literal_type->equals(*array_type))
            literal = addTypeConversionToAST(std::move(literal), array_type->getName());

        ASTPtr expression = makeASTFunction("arrayJoin", std::move(literal));
        if (num_columns > 1)
            expression = makeASTFunction("untuple", std::move(expression));

        return makeSubquery(std::move(expression));
    }
};

using ExecuteSetSubqueriesVisitor = InDepthNodeVisitor<ExecuteSetSubqueriesMatcher, true>;

}

FirstNonDeterministicFunctionResult findFirstNonDeterministicFunction(const MutationCommand & command, ContextPtr context)
{
    FirstNonDeterministicFunctionMatcher::Data finder_data{context, {}};

    switch (command.type)
    {
        case MutationCommand::UPDATE:
        {
            auto alter = command.ast();
            auto update_assignments_ast = alter->update_assignments->clone();
            FirstNonDeterministicFunctionFinder(finder_data).visit(update_assignments_ast);

            if (finder_data.result.nondeterministic_function_name)
                return finder_data.result;

            ASTPtr predicate_ast(alter->predicate);
            FirstNonDeterministicFunctionFinder(finder_data).visit(predicate_ast);
            return finder_data.result;
        }

        case MutationCommand::DELETE:
        {
            auto alter = command.ast();
            ASTPtr predicate_ast(alter->predicate);
            FirstNonDeterministicFunctionFinder(finder_data).visit(predicate_ast);
            return finder_data.result;
        }

        default:
            break;
    }

    return {};
}

ASTPtr replaceNonDeterministicToScalars(const ASTAlterCommand & alter_command, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::mutations_execute_subqueries_on_initiator] && !settings[Setting::mutations_execute_nondeterministic_on_initiator])
        return nullptr;

    auto query = alter_command.clone();
    auto & new_alter_command = *query->as<ASTAlterCommand>();

    auto remove_child = [](auto & children, IAST *& erase_ptr)
    {
        auto it = std::find_if(children.begin(), children.end(), [&](const auto & ptr) { return ptr.get() == erase_ptr; });
        erase_ptr = nullptr;
        children.erase(it);
    };
    auto visit = [&](auto & visitor)
    {
        if (new_alter_command.update_assignments)
        {
            ASTPtr update_assignments = new_alter_command.update_assignments->clone();
            remove_child(new_alter_command.children, new_alter_command.update_assignments);
            visitor.visit(update_assignments);
            new_alter_command.update_assignments = new_alter_command.children.emplace_back(std::move(update_assignments)).get();
        }
        if (new_alter_command.predicate)
        {
            ASTPtr predicate = new_alter_command.predicate->clone();
            remove_child(new_alter_command.children, new_alter_command.predicate);
            visitor.visit(predicate);
            new_alter_command.predicate = new_alter_command.children.emplace_back(std::move(predicate)).get();
        }
    };

    if (settings[Setting::mutations_execute_subqueries_on_initiator])
    {
        Scalars scalars;
        Scalars local_scalars;

        ExecuteScalarSubqueriesVisitor::Data data{
            WithContext{context},
            /*subquery_depth=*/0,
            scalars,
            local_scalars,
            /*only_analyze=*/false,
            /*is_create_parameterized_view=*/false,
            /*replace_only_to_literals=*/true,
            settings[Setting::mutations_max_literal_size_to_replace]};

        ExecuteScalarSubqueriesVisitor visitor(data);
        visit(visitor);

        ExecuteSetSubqueriesVisitor::Data set_data{context, settings[Setting::mutations_max_literal_size_to_replace], {}, {}};
        ExecuteSetSubqueriesMatcher::collectAliases(new_alter_command, set_data.aliases);
        ExecuteSetSubqueriesVisitor set_visitor(set_data);
        visit(set_visitor);
    }

    if (settings[Setting::mutations_execute_nondeterministic_on_initiator])
    {
        ExecuteNonDeterministicConstFunctionsVisitor::Data data{context, settings[Setting::mutations_max_literal_size_to_replace]};

        ExecuteNonDeterministicConstFunctionsVisitor visitor(data);
        visit(visitor);
    }

    return query;
}

}
