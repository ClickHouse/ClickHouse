#include <Parsers/Kusto/KQLTranslator.h>

#include <Parsers/Kusto/KQLFunctions.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace
{

ASTPtr lit(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr ident(const String & name)
{
    return make_intrusive<ASTIdentifier>(name);
}

ASTPtr expressionList(ASTs children)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(children);
    return list;
}

[[noreturn]] void unsupported(const String & message)
{
    throw Exception(ErrorCodes::SYNTAX_ERROR, "{}", message);
}

/// A `*` with `EXCEPT (...)` attached.
ASTPtr asteriskExcept(const std::vector<String> & names)
{
    auto asterisk = make_intrusive<ASTAsterisk>();
    if (names.empty())
        return asterisk;

    auto except = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & name : names)
        except->children.push_back(ident(name));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(except);
    asterisk->transformers = transformers;
    asterisk->children.push_back(transformers);
    return asterisk;
}

/// Kusto's default name for an aggregate result: `count()` becomes `count_`,
/// `sum(Damage)` becomes `sum_Damage`.
String defaultAggregateName(const ASTPtr & expression)
{
    const auto * function = expression->as<ASTFunction>();
    if (!function)
        return {};

    String base = function->name;
    /// The AST already carries ClickHouse names, so recover Kusto's spelling for the common ones.
    static const std::map<String, String> kusto_names{
        {"count", "count"},     {"countIf", "countif"}, {"sum", "sum"},         {"sumIf", "sumif"},
        {"avg", "avg"},         {"avgIf", "avgif"},     {"min", "min"},         {"minIf", "minif"},
        {"max", "max"},         {"maxIf", "maxif"},     {"uniq", "dcount"},     {"uniqIf", "dcountif"},
        {"groupArray", "make_list"}, {"groupUniqArray", "make_set"}, {"any", "take_any"},
        {"stddevSamp", "stdev"}, {"varSamp", "variance"}, {"argMax", "arg_max"}, {"argMin", "arg_min"},
        {"quantile", "percentile"},
    };
    if (auto it = kusto_names.find(function->name); it != kusto_names.end())
        base = it->second;

    if (!function->arguments || function->arguments->children.empty())
        return base + "_";

    const auto & first = function->arguments->children.front();
    if (const auto * identifier = first->as<ASTIdentifier>())
        return base + "_" + identifier->shortName();
    return base + "_";
}

/// Kusto names a `by` key after the column it is derived from: `bin(Timestamp, 1d)` stays
/// `Timestamp`.
String defaultGroupKeyName(const ASTPtr & expression, size_t ordinal)
{
    if (const auto * identifier = expression->as<ASTIdentifier>())
        return identifier->shortName();

    if (const auto * function = expression->as<ASTFunction>(); function && function->arguments)
        for (const auto & argument : function->arguments->children)
            if (const auto * identifier = argument->as<ASTIdentifier>())
                return identifier->shortName();

    return fmt::format("Column{}", ordinal);
}

/// Where a clause sits in SQL evaluation order. An operator that needs a slot at or before
/// one already filled has to start a new select over the current one.
enum class Stage : uint8_t
{
    Source = 0,
    Where = 1,
    Aggregate = 2,
    Project = 3,
    Order = 4,
    Limit = 5,
};

class SelectBuilder
{
public:
    void setTableExpression(ASTPtr table_expression)
    {
        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = table_expression;
        element->children.push_back(table_expression);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(element);
        select->setExpression(ASTSelectQuery::Expression::TABLES, tables);
    }

    /// Names the subquery this select reads from. A joined subquery must be named, or
    /// `joined_subquery_requires_alias` rejects the query.
    void setSourceAlias(const String & alias)
    {
        if (!select->tables() || select->tables()->children.empty())
            return;
        auto * element = select->tables()->children.front()->as<ASTTablesInSelectQueryElement>();
        if (!element || !element->table_expression)
            return;
        if (auto * expression = element->table_expression->as<ASTTableExpression>(); expression && expression->subquery)
            expression->subquery->setAlias(alias);
    }

    /// Adds a JOIN element beside the existing table.
    void addJoin(ASTPtr table_expression, ASTPtr table_join)
    {
        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = table_expression;
        element->table_join = table_join;
        element->children.push_back(table_join);
        element->children.push_back(table_expression);
        select->tables()->children.push_back(element);
    }

    void addWhere(ASTPtr predicate)
    {
        if (stage > Stage::Where)
            nest();

        if (ASTPtr existing = select->where())
            predicate = makeASTFunction("and", existing, predicate);

        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(predicate));
        stage = Stage::Where;
    }

    void setProjection(ASTs columns, bool distinct = false)
    {
        if (stage >= Stage::Project)
            nest();
        select->setExpression(ASTSelectQuery::Expression::SELECT, expressionList(std::move(columns)));
        select->distinct = distinct;
        stage = Stage::Project;
    }

    void setAggregation(ASTs columns, ASTs group_by)
    {
        if (stage >= Stage::Aggregate)
            nest();
        select->setExpression(ASTSelectQuery::Expression::SELECT, expressionList(std::move(columns)));
        if (!group_by.empty())
            select->setExpression(ASTSelectQuery::Expression::GROUP_BY, expressionList(std::move(group_by)));
        stage = Stage::Project;
    }

    void setOrderBy(ASTs order_by)
    {
        if (stage >= Stage::Order)
            nest();
        select->setExpression(ASTSelectQuery::Expression::ORDER_BY, expressionList(std::move(order_by)));
        stage = Stage::Order;
    }

    void setLimit(ASTPtr limit)
    {
        if (stage >= Stage::Limit)
            nest();
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit));
        stage = Stage::Limit;
    }

    void setLimitBy(ASTPtr length, ASTs keys)
    {
        if (stage >= Stage::Limit)
            nest();
        select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, std::move(length));
        select->setExpression(ASTSelectQuery::Expression::LIMIT_BY, expressionList(std::move(keys)));
        stage = Stage::Limit;
    }

    /// Current select becomes the FROM of a fresh one.
    void nest()
    {
        finishSelectList();

        auto inner = make_intrusive<ASTSelectWithUnionQuery>();
        inner->list_of_selects = expressionList({select});
        inner->children.push_back(inner->list_of_selects);

        auto subquery = make_intrusive<ASTSubquery>(ASTPtr(inner));
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = subquery;
        table_expression->children.push_back(subquery);

        select = make_intrusive<ASTSelectQuery>();
        stage = Stage::Source;
        setTableExpression(table_expression);
    }

    ASTPtr build()
    {
        finishSelectList();
        select->normalizeChildrenOrder();

        auto result = make_intrusive<ASTSelectWithUnionQuery>();
        result->list_of_selects = expressionList({select});
        result->children.push_back(result->list_of_selects);
        return result;
    }

private:
    void finishSelectList()
    {
        if (!select->select())
            select->setExpression(ASTSelectQuery::Expression::SELECT, expressionList({make_intrusive<ASTAsterisk>()}));
        select->normalizeChildrenOrder();
    }

    boost::intrusive_ptr<ASTSelectQuery> select = make_intrusive<ASTSelectQuery>();
    Stage stage = Stage::Source;
};

class Translator
{
public:
    ASTPtr translate(const KQLTabularExpression & query)
    {
        SelectBuilder builder;
        buildSource(builder, *query.source);

        for (const auto & op : query.operators)
        {
            /// `union` is the one operator that replaces the input rather than transforming
            /// it: everything to the left of the pipe becomes its first operand.
            if (op->kind == KQLOperatorKind::Union)
            {
                std::vector<ASTPtr> operands{builder.build()};
                for (const auto & input : op->inputs)
                    operands.push_back(translate(*input));

                builder = SelectBuilder{};
                builder.setTableExpression(unionTableExpression(operands));
                continue;
            }

            applyOperator(builder, *op);
        }

        return builder.build();
    }

private:
    /// Wraps a whole tabular expression as a subquery table expression, for FROM and JOIN.
    ASTPtr asTableExpression(const KQLTabularExpression & query, const String & alias = {})
    {
        auto subquery = make_intrusive<ASTSubquery>(translate(query)->clone());
        if (!alias.empty())
            subquery->setAlias(alias);
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = subquery;
        table_expression->children.push_back(subquery);
        return table_expression;
    }

    void buildSource(SelectBuilder & builder, const KQLSource & source)
    {
        switch (source.kind)
        {
            case KQLSourceKind::Table:
            {
                auto table = source.database.empty() ? make_intrusive<ASTTableIdentifier>(source.table)
                                                     : make_intrusive<ASTTableIdentifier>(source.database, source.table);
                auto table_expression = make_intrusive<ASTTableExpression>();
                table_expression->database_and_table_name = table;
                table_expression->children.push_back(table);
                builder.setTableExpression(table_expression);
                return;
            }

            case KQLSourceKind::Print:
            {
                /// `print` has no input, so it becomes a select with no FROM.
                ASTs columns;
                size_t ordinal = 1;
                for (const auto & projection : source.projections)
                    columns.push_back(aliased(projection, ordinal));
                builder.setProjection(std::move(columns));
                return;
            }

            case KQLSourceKind::DataTable:
            {
                builder.setTableExpression(buildDataTable(source));
                return;
            }

            case KQLSourceKind::Range:
            {
                builder.setTableExpression(buildRange(source));
                return;
            }

            case KQLSourceKind::Subquery:
            {
                builder.setTableExpression(asTableExpression(*source.inputs.front()));
                return;
            }

            case KQLSourceKind::Union:
            {
                builder.setTableExpression(buildUnion(source.inputs));
                return;
            }
        }
    }

    /// `datatable (a:long, b:string) [1, 'x', 2, 'y']` becomes the `values` table function.
    ASTPtr buildDataTable(const KQLSource & source)
    {
        String structure;
        for (size_t i = 0; i < source.column_names.size(); ++i)
        {
            if (i)
                structure += ", ";
            structure += backQuoteIfNeed(source.column_names[i]);
            structure += " ";
            structure += source.column_types[i];
        }

        auto values = makeASTFunction("values");
        values->arguments->children.push_back(lit(structure));

        const size_t width = source.column_names.size();
        for (size_t offset = 0; offset < source.values.size(); offset += width)
        {
            /// With one column the `values` table function wants the value itself: a
            /// one-element `tuple(x)` really is a Tuple, and would not match the declared
            /// column type.
            if (width == 1)
            {
                values->arguments->children.push_back(source.values[offset]);
                continue;
            }

            auto row = makeASTFunction("tuple");
            for (size_t i = 0; i < width; ++i)
                row->arguments->children.push_back(source.values[offset + i]);
            values->arguments->children.push_back(row);
        }

        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->table_function = values;
        table_expression->children.push_back(values);
        return table_expression;
    }

    /// `range x from a to b step s` becomes
    /// `(SELECT a + s * number AS x FROM numbers(count))`.
    ASTPtr buildRange(const KQLSource & source)
    {
        /// Only the runtime sees whether the range goes over numbers, datetimes or timespans,
        /// so both the row count and the scaling of the step dispatch there: `kqlRangeCount`
        /// counts a temporal range in nanoseconds, and `kqlMultiply` scales a timespan.
        ASTPtr count = makeASTFunction("kqlRangeCount", source.range_from, source.range_to, source.range_step);

        auto numbers = makeASTFunction("numbers", count);
        auto numbers_expression = make_intrusive<ASTTableExpression>();
        numbers_expression->table_function = numbers;
        numbers_expression->children.push_back(numbers);

        SelectBuilder inner;
        inner.setTableExpression(numbers_expression);

        /// `number` is `numbers()`'s output column.
        ASTPtr value = makeASTFunction(
            "plus", source.range_from->clone(), makeASTFunction("kqlMultiply", source.range_step->clone(), ident("number")));
        value->setAlias(source.range_column);
        inner.setProjection({value});

        auto subquery = make_intrusive<ASTSubquery>(inner.build());
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = subquery;
        table_expression->children.push_back(subquery);
        return table_expression;
    }

    ASTPtr buildUnion(const std::vector<KQLTabularExpressionPtr> & inputs)
    {
        std::vector<ASTPtr> operands;
        operands.reserve(inputs.size());
        for (const auto & input : inputs)
            operands.push_back(translate(*input));
        return unionTableExpression(operands);
    }

    /// Combines already-translated operands into one `UNION ALL` used as a table expression.
    /// Kusto's `union` concatenates without deduplicating, which is `UNION ALL`.
    static ASTPtr unionTableExpression(const std::vector<ASTPtr> & operands)
    {
        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        union_query->union_mode = SelectUnionMode::UNION_ALL;
        union_query->is_normalized = true;
        union_query->list_of_selects = make_intrusive<ASTExpressionList>();
        union_query->children.push_back(union_query->list_of_selects);

        for (const auto & operand : operands)
        {
            /// Splice each operand's own selects in, so nested unions stay flat.
            for (const auto & child : operand->as<ASTSelectWithUnionQuery>()->list_of_selects->children)
                union_query->list_of_selects->children.push_back(child);
        }

        const size_t operand_count = union_query->list_of_selects->children.size();
        union_query->list_of_modes.assign(operand_count ? operand_count - 1 : 0, SelectUnionMode::UNION_ALL);
        union_query->set_of_modes.insert(SelectUnionMode::UNION_ALL);

        auto subquery = make_intrusive<ASTSubquery>(ASTPtr(union_query));
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = subquery;
        table_expression->children.push_back(subquery);
        return table_expression;
    }

    /// Applies the alias a named expression carries, inventing one when KQL would.
    static ASTPtr aliased(const KQLNamedExpression & named, size_t & ordinal)
    {
        /// Always a fresh node: the same KQL expression can reach the translator twice (a
        /// `let`-bound tabular expression used in two places), and the two copies must not
        /// share nodes with each other.
        ASTPtr expression = named.expression->clone();
        if (!named.alias.empty())
            expression->setAlias(named.alias);
        else if (!expression->as<ASTIdentifier>() && !expression->as<ASTAsterisk>())
            expression->setAlias(fmt::format("Column{}", ordinal));
        ++ordinal;
        return expression;
    }

    void applyOperator(SelectBuilder & builder, const KQLOperator & op)
    {
        switch (op.kind)
        {
            case KQLOperatorKind::Where:
                builder.addWhere(op.predicate);
                return;

            case KQLOperatorKind::Extend:
            {
                /// `extend` keeps every existing column and adds new ones. A name that already
                /// exists must not appear twice, so it is excluded from the `*`.
                std::vector<String> shadowed;
                for (const auto & named : op.expressions)
                    if (!named.alias.empty())
                        shadowed.push_back(named.alias);

                ASTs columns{asteriskExcept(shadowed)};
                size_t ordinal = 1;
                for (const auto & named : op.expressions)
                    columns.push_back(aliased(named, ordinal));
                builder.setProjection(std::move(columns));
                return;
            }

            case KQLOperatorKind::Project:
            {
                ASTs columns;
                size_t ordinal = 1;
                for (const auto & named : op.expressions)
                    columns.push_back(aliased(named, ordinal));
                builder.setProjection(std::move(columns));
                return;
            }

            case KQLOperatorKind::ProjectAway:
            {
                for (const auto & pattern : op.column_patterns)
                    if (pattern.contains('*'))
                        unsupported("Wildcards in 'project-away' are not supported");
                builder.setProjection({asteriskExcept(op.column_patterns)});
                return;
            }

            case KQLOperatorKind::ProjectKeep:
            {
                ASTs columns;
                for (const auto & pattern : op.column_patterns)
                {
                    if (pattern.contains('*'))
                        unsupported("Wildcards in 'project-keep' are not supported");
                    columns.push_back(ident(pattern));
                }
                builder.setProjection(std::move(columns));
                return;
            }

            case KQLOperatorKind::ProjectRename:
            {
                /// The renamed columns move to the end of the row: the original position
                /// cannot be reproduced without knowing the schema at parse time.
                std::vector<String> old_names;
                ASTs columns;
                for (const auto & [new_name, old_name] : op.renames)
                    old_names.push_back(old_name);
                columns.push_back(asteriskExcept(old_names));
                for (const auto & [new_name, old_name] : op.renames)
                {
                    ASTPtr column = ident(old_name);
                    column->setAlias(new_name);
                    columns.push_back(column);
                }
                builder.setProjection(std::move(columns));
                return;
            }

            case KQLOperatorKind::Summarize:
            {
                ASTs columns;
                ASTs group_by;
                ASTs precomputed;
                size_t ordinal = 1;

                for (const auto & named : op.by_expressions)
                {
                    ASTPtr key = named.expression;
                    const String name = named.alias.empty() ? defaultGroupKeyName(key, ordinal) : named.alias;

                    /// A bare column groups by itself.
                    if (key->as<ASTIdentifier>() && named.alias.empty())
                    {
                        columns.push_back(key->clone());
                        group_by.push_back(key->clone());
                        ++ordinal;
                        continue;
                    }

                    /// Anything computed is evaluated one level down under a name of our own.
                    /// Kusto names `bin(Timestamp, 1h)` after its source column, and grouping
                    /// by `Timestamp` when `Timestamp AS` is also in the select list either
                    /// fails to resolve or - under `prefer_column_name_to_alias` - silently
                    /// groups by the raw column instead of the binned value.
                    const String internal = fmt::format("__kql_groupkey_{}", ordinal);
                    ++ordinal;

                    ASTPtr computed = key->clone();
                    computed->setAlias(internal);
                    precomputed.push_back(computed);

                    ASTPtr projected = ident(internal);
                    projected->setAlias(name);
                    columns.push_back(projected);
                    group_by.push_back(ident(internal));
                }

                if (!precomputed.empty())
                {
                    ASTs with_keys{make_intrusive<ASTAsterisk>()};
                    for (auto & key : precomputed)
                        with_keys.push_back(std::move(key));
                    builder.setProjection(std::move(with_keys));
                }

                for (const auto & named : op.expressions)
                {
                    ASTPtr aggregate = named.expression->clone();
                    const String name = named.alias.empty() ? defaultAggregateName(aggregate) : named.alias;
                    if (!name.empty())
                        aggregate->setAlias(name);
                    columns.push_back(aggregate);
                }

                builder.setAggregation(std::move(columns), std::move(group_by));
                return;
            }

            case KQLOperatorKind::Sort:
            case KQLOperatorKind::Top:
            {
                ASTs order_by;
                for (const auto & item : op.sort_items)
                {
                    auto element = make_intrusive<ASTOrderByElement>();
                    element->direction = item.descending ? -1 : 1;
                    element->nulls_direction_was_explicitly_specified = true;
                    /// `nulls_direction` equal to `direction` puts nulls last.
                    element->nulls_direction = item.nulls_first ? -element->direction : element->direction;
                    element->children.push_back(item.expression);
                    order_by.push_back(element);
                }
                builder.setOrderBy(std::move(order_by));

                if (op.kind == KQLOperatorKind::Top)
                    builder.setLimit(op.limit);
                return;
            }

            case KQLOperatorKind::Take:
                builder.setLimit(op.limit);
                return;

            case KQLOperatorKind::Distinct:
            {
                ASTs columns;
                if (op.expressions.empty())
                {
                    columns.push_back(make_intrusive<ASTAsterisk>());
                }
                else
                {
                    size_t ordinal = 1;
                    for (const auto & named : op.expressions)
                        columns.push_back(aliased(named, ordinal));
                }
                builder.setProjection(std::move(columns), /*distinct=*/true);
                return;
            }

            case KQLOperatorKind::Count:
            {
                ASTPtr count = makeASTFunction("count");
                count->setAlias(op.alias);
                builder.setAggregation({count}, {});
                return;
            }

            case KQLOperatorKind::MvExpand:
            {
                std::vector<String> expanded;
                ASTs columns;
                ASTs arrays;
                for (const auto & named : op.expressions)
                {
                    const auto * identifier = named.expression->as<ASTIdentifier>();
                    if (!identifier && named.alias.empty())
                        unsupported("'mv-expand' of an expression needs an explicit name, as in 'mv-expand x = f(c)'");
                    const String name = named.alias.empty() ? identifier->shortName() : named.alias;
                    if (identifier)
                        expanded.push_back(identifier->shortName());
                    arrays.push_back(named.expression);
                    columns.push_back(ident(name));
                }

                /// Kusto expands multiple arrays in lockstep and pads the shorter ones with
                /// NULL. One `arrayJoin` over `arrayZipUnaligned` preserves that row shape;
                /// independent `arrayJoin` calls would form a Cartesian product.
                ASTPtr zipped = makeASTFunction("arrayZipUnaligned", std::move(arrays));
                ASTPtr expansion = makeASTFunction("arrayJoin", std::move(zipped));
                expansion->setAlias("kql_mv_expand");
                ASTs expanded_columns;
                for (size_t i = 0; i < columns.size(); ++i)
                {
                    ASTPtr element = makeASTFunction("tupleElement", ident("kql_mv_expand"), lit(i + 1));
                    element->setAlias(columns[i]->as<ASTIdentifier>()->shortName());
                    expanded_columns.push_back(std::move(element));
                }

                ASTs intermediate{asteriskExcept(expanded)};
                intermediate.push_back(std::move(expansion));
                builder.setProjection(std::move(intermediate));

                ASTs all{asteriskExcept({"kql_mv_expand"})};
                for (auto & column : expanded_columns)
                    all.push_back(std::move(column));
                builder.setProjection(std::move(all));
                return;
            }

            case KQLOperatorKind::Join:
            {
                applyJoin(builder, op);
                return;
            }

            case KQLOperatorKind::Union:
                /// Handled in `translate`, which has the left-hand pipeline to hand.
                return;

            case KQLOperatorKind::As:
            case KQLOperatorKind::Render:
                /// Neither changes the rows. `render` is a client-side hint in Kusto too, and
                /// `as` only names the result for a later reference this dialect does not have.
                return;
        }
    }

    void applyJoin(SelectBuilder & builder, const KQLOperator & op)
    {
        /// `innerunique` - Kusto's default - keeps one left row per value of the join keys
        /// before matching, which is exactly `LIMIT 1 BY` on the left operand.
        if (op.join_kind == KQLJoinKind::InnerUnique)
        {
            ASTs keys;
            for (const auto & [left, right] : op.join_keys)
                keys.push_back(ident(left));
            builder.setLimitBy(lit(1u), std::move(keys));
        }

        /// Everything accumulated so far becomes the left operand, so the join sits at the
        /// top of a fresh select. Both operands are subqueries, and a joined subquery has to
        /// be named.
        builder.nest();
        ++join_ordinal;
        builder.setSourceAlias(fmt::format("kql_left_{}", join_ordinal));

        auto table_join = make_intrusive<ASTTableJoin>();
        table_join->strictness = JoinStrictness::All;
        switch (op.join_kind)
        {
            case KQLJoinKind::Inner:
            case KQLJoinKind::InnerUnique:
                table_join->kind = JoinKind::Inner;
                break;
            case KQLJoinKind::LeftOuter:
                table_join->kind = JoinKind::Left;
                break;
            case KQLJoinKind::RightOuter:
                table_join->kind = JoinKind::Right;
                break;
            case KQLJoinKind::FullOuter:
                table_join->kind = JoinKind::Full;
                break;
            case KQLJoinKind::LeftSemi:
                table_join->kind = JoinKind::Left;
                table_join->strictness = JoinStrictness::Semi;
                break;
            case KQLJoinKind::RightSemi:
                table_join->kind = JoinKind::Right;
                table_join->strictness = JoinStrictness::Semi;
                break;
            case KQLJoinKind::LeftAnti:
                table_join->kind = JoinKind::Left;
                table_join->strictness = JoinStrictness::Anti;
                break;
            case KQLJoinKind::RightAnti:
                table_join->kind = JoinKind::Right;
                table_join->strictness = JoinStrictness::Anti;
                break;
        }

        /// Do not use `USING` even when both key names agree: unlike Kusto it coalesces the
        /// matching columns, while Kusto joins that return both sides keep both key columns.
        {
            /// Qualify both sides with the subquery aliases: `on $left.a == $right.b` must
            /// stay unambiguous when both inputs expose both names.
            ASTPtr condition;
            for (const auto & [left, right] : op.join_keys)
            {
                ASTPtr left_key = make_intrusive<ASTIdentifier>(std::vector<String>{fmt::format("kql_left_{}", join_ordinal), left});
                ASTPtr right_key = make_intrusive<ASTIdentifier>(std::vector<String>{fmt::format("kql_right_{}", join_ordinal), right});
                ASTPtr equality = makeASTFunction("equals", std::move(left_key), std::move(right_key));
                condition = condition ? ASTPtr(makeASTFunction("and", condition, equality)) : equality;
            }
            table_join->on_expression = condition;
            table_join->children.push_back(condition);
        }

        builder.addJoin(asTableExpression(*op.inputs.front(), fmt::format("kql_right_{}", join_ordinal)), table_join);
    }

    size_t join_ordinal = 0;
};

}

ASTPtr translateKQLQuery(const KQLTabularExpression & query)
{
    Translator translator;
    return translator.translate(query);
}

}
