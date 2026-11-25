#include <Interpreters/Q/compileQProgram.h>

#include <Interpreters/Q/QCompiler.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTime64.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/Q/ASTQQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
#include <base/arithmeticOverflow.h>
#include <base/scope_guard.h>

#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_expanded_ast_elements;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_BIG_AST;
    extern const int TOO_DEEP_RECURSION;
    extern const int ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}

namespace QCompilerInternals
{

namespace
{

/// The elements copied into the generated query by the compilation that runs in this thread, see `cloneForQuery`.
/// The copies are made by helpers that build pieces of a query (`withAlias`, `QSelectBuilder::build`) all over
/// the compiler, so the budget is found through the thread instead of being passed to every one of them.
/// It is held by value (`compileProgram` sets it and puts the previous one back), and no limit means no compilation.
struct CopyBudget
{
    size_t max_elements = 0;
    size_t copied_elements = 0;
};

thread_local CopyBudget copy_budget;

[[noreturn]] void throwTooBigExpansion(size_t max_elements)
{
    throw Exception(ErrorCodes::TOO_BIG_AST,
        "The q program expands into a query of more than {} elements (see the setting `max_expanded_ast_elements`)", max_elements);
}

}

ASTPtr cloneForQuery(const ASTPtr & ast)
{
    if (copy_budget.max_elements)
    {
        /// Counting before copying keeps the work within the limit: nothing is copied once the budget is spent.
        std::vector<const IAST *> pending{ast.get()};
        while (!pending.empty())
        {
            const IAST * node = pending.back();
            pending.pop_back();
            if (++copy_budget.copied_elements > copy_budget.max_elements)
                throwTooBigExpansion(copy_budget.max_elements);
            for (const auto & child : node->children)
                if (child)
                    pending.push_back(child.get());
        }
    }
    return ast->clone();
}

void throwQError(int code, size_t offset, const String & message)
{
    throw Exception(code, "{} (at position {} of the q program)", message, offset + 1);
}

ASTPtr makeIdentifier(const String & name)
{
    return make_intrusive<ASTIdentifier>(name);
}

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr makeLambda(const Names & params, ASTPtr body)
{
    return makeASTLambda(params, std::move(body));
}

ASTPtr makeExpressionList(ASTs children)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(children);
    return list;
}

ASTPtr withAlias(ASTPtr ast, const String & alias)
{
    ASTPtr result = cloneForQuery(ast);
    if (auto * with_alias = dynamic_cast<ASTWithAlias *>(result.get()))
        with_alias->setAlias(alias);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot name the expression '{}'", alias);
    return result;
}

ASTPtr makeSubqueryTableExpression(ASTPtr query, const String & alias)
{
    auto subquery = make_intrusive<ASTSubquery>(std::move(query));
    if (!alias.empty())
        subquery->setAlias(alias);
    auto table_expression = make_intrusive<ASTTableExpression>();
    table_expression->subquery = subquery;
    table_expression->children.push_back(subquery);
    return table_expression;
}

ASTPtr makeTableIdentifierExpression(const String & database, const String & table)
{
    auto identifier = database.empty() ? make_intrusive<ASTTableIdentifier>(table) : make_intrusive<ASTTableIdentifier>(database, table);
    auto table_expression = make_intrusive<ASTTableExpression>();
    table_expression->database_and_table_name = identifier;
    table_expression->children.push_back(identifier);
    return table_expression;
}

ASTPtr QSelectBuilder::build() const
{
    auto query = make_intrusive<ASTSelectQuery>();
    query->distinct = distinct;

    ASTs columns;
    for (const auto & column : select)
        columns.push_back(column);
    if (columns.empty())
        columns.push_back(make_intrusive<ASTAsterisk>());
    query->setExpression(ASTSelectQuery::Expression::SELECT, makeExpressionList(std::move(columns)));

    if (from)
    {
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = cloneForQuery(from);
        element->children.push_back(element->table_expression);
        tables->children.push_back(element);

        for (const auto & [table_expression, table_join] : joins)
        {
            auto join_element = make_intrusive<ASTTablesInSelectQueryElement>();
            join_element->table_join = table_join;
            join_element->table_expression = table_expression;
            join_element->children.push_back(table_join);
            join_element->children.push_back(table_expression);
            tables->children.push_back(join_element);
        }

        if (!array_join.empty())
        {
            auto array_join_ast = make_intrusive<ASTArrayJoin>();
            array_join_ast->kind = ASTArrayJoin::Kind::Inner;
            array_join_ast->expression_list = makeExpressionList(array_join);
            array_join_ast->children.push_back(array_join_ast->expression_list);
            auto array_join_element = make_intrusive<ASTTablesInSelectQueryElement>();
            array_join_element->array_join = array_join_ast;
            array_join_element->children.push_back(array_join_ast);
            tables->children.push_back(array_join_element);
        }

        query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
    }

    if (where)
        query->setExpression(ASTSelectQuery::Expression::WHERE, cloneForQuery(where));
    if (!group_by.empty())
        query->setExpression(ASTSelectQuery::Expression::GROUP_BY, makeExpressionList(group_by));
    if (!order_by.empty())
    {
        ASTs elements;
        for (const auto & [expression, descending] : order_by)
        {
            auto element = make_intrusive<ASTOrderByElement>();
            element->direction = descending ? -1 : 1;
            element->nulls_direction = element->direction;
            element->children.push_back(expression);
            elements.push_back(element);
        }
        query->setExpression(ASTSelectQuery::Expression::ORDER_BY, makeExpressionList(std::move(elements)));
    }
    if (limit)
        query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, cloneForQuery(limit));
    if (offset)
        query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, cloneForQuery(offset));
    query->normalizeChildrenOrder();

    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    result->list_of_selects = makeExpressionList({query});
    result->children.push_back(result->list_of_selects);
    return result;
}

String qTypeToClickHouseType(QType type)
{
    switch (type)
    {
        case QType::Boolean: return "Bool";
        case QType::Guid: return "UUID";
        case QType::Byte: return "UInt8";
        case QType::Short: return "Int16";
        case QType::Int: return "Int32";
        case QType::Long: return "Int64";
        case QType::Real: return "Float32";
        case QType::Float: return "Float64";
        case QType::Char: return "String";
        case QType::Symbol: return "String";
        case QType::Timestamp: return "DateTime64(9)";
        case QType::Month: return "Date32";
        case QType::Date: return "Date32";
        case QType::Datetime: return "DateTime64(3)";
        case QType::Timespan: return "Time64(9)";
        case QType::Minute: return "Time";
        case QType::Second: return "Time";
        case QType::Time: return "Time64(3)";
        case QType::Mixed: break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The q type of general lists has no ClickHouse equivalent");
}

std::optional<QType> dataTypeToQType(const DataTypePtr & type)
{
    DataTypePtr inner = removeLowCardinalityAndNullable(type);
    if (const auto * array = typeid_cast<const DataTypeArray *>(inner.get()))
        return dataTypeToQType(array->getNestedType());

    switch (inner->getTypeId())
    {
        case TypeIndex::UInt8:
            return inner->getName() == "Bool" ? QType::Boolean : QType::Byte;
        case TypeIndex::Int8:
        case TypeIndex::Int16:
            return QType::Short;
        case TypeIndex::Int32:
        case TypeIndex::UInt16:
            return QType::Int;
        case TypeIndex::Int64:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
            return QType::Long;
        case TypeIndex::Float32:
            return QType::Real;
        case TypeIndex::Float64:
            return QType::Float;
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
            return QType::Symbol;
        case TypeIndex::Date:
        case TypeIndex::Date32:
            return QType::Date;
        case TypeIndex::DateTime:
            return QType::Datetime;
        case TypeIndex::DateTime64:
        {
            /// A datetime of q has milliseconds, and a timestamp has nanoseconds.
            const auto & datetime64 = typeid_cast<const DataTypeDateTime64 &>(*inner);
            return datetime64.getScale() > 3 ? QType::Timestamp : QType::Datetime;
        }
        case TypeIndex::Time:
            return QType::Second;
        case TypeIndex::Time64:
        {
            const auto & time64 = typeid_cast<const DataTypeTime64 &>(*inner);
            return time64.getScale() > 3 ? QType::Timespan : QType::Time;
        }
        case TypeIndex::UUID:
            return QType::Guid;
        default:
            return std::nullopt;
    }
}

size_t arrayDepth(const DataTypePtr & type, bool & is_string)
{
    DataTypePtr inner = removeLowCardinalityAndNullable(type);
    size_t depth = 0;
    while (const auto * array = typeid_cast<const DataTypeArray *>(inner.get()))
    {
        ++depth;
        inner = removeLowCardinalityAndNullable(array->getNestedType());
    }
    is_string = inner->getTypeId() == TypeIndex::String || inner->getTypeId() == TypeIndex::FixedString;
    return depth;
}

namespace
{

/// The days since 1970-01-01 to a civil date.
void civilFromDays(Int64 days, Int64 & year, Int64 & month, Int64 & day)
{
    days += 719468;
    const Int64 era = (days >= 0 ? days : days - 146096) / 146097;
    const Int64 day_of_era = days - era * 146097;
    const Int64 year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    const Int64 day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    const Int64 mp = (5 * day_of_year + 2) / 153;
    day = day_of_year - (153 * mp + 2) / 5 + 1;
    month = mp < 10 ? mp + 3 : mp - 9;
    year = year_of_era + era * 400 + (month <= 2);
}

constexpr Int64 days_1970_to_2000 = 10957;
constexpr Int64 nanoseconds_in_day = 86'400'000'000'000;
constexpr Int64 milliseconds_in_day = 86'400'000;

Int64 floorDiv(Int64 a, Int64 b)
{
    Int64 q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0)))
        --q;
    return q;
}

/// Splits a count of units since an epoch into the whole days and the non-negative rest of the day.
/// It does not multiply the days back, which would overflow for the first day of the 64-bit range.
void splitDays(Int64 units, Int64 units_in_day, Int64 & days, Int64 & rest)
{
    days = units / units_in_day;
    rest = units % units_in_day;
    if (rest < 0)
    {
        rest += units_in_day;
        --days;
    }
}

String formatDate(Int64 days_since_2000)
{
    Int64 year = 0;
    Int64 month = 0;
    Int64 day = 0;
    civilFromDays(days_since_2000 + days_1970_to_2000, year, month, day);
    return fmt::format("{:04}-{:02}-{:02}", year, month, day);
}

/// Formats a time of day or a duration as `[-]h:mm:ss[.fff]`. The value is a count of the units of `10^-digits` seconds
/// (seconds, milliseconds, or nanoseconds), so it is formatted in its own unit, without a conversion that could overflow.
String formatClock(Int64 units, int digits)
{
    UInt64 units_in_second = 1;
    for (int i = 0; i < digits; ++i)
        units_in_second *= 10;
    bool negative = units < 0;
    UInt64 value = negative ? static_cast<UInt64>(-(units + 1)) + 1 : static_cast<UInt64>(units);
    UInt64 fraction = value % units_in_second;
    UInt64 seconds = value / units_in_second;
    String result = fmt::format("{}{:02}:{:02}:{:02}", negative ? "-" : "", seconds / 3600, seconds / 60 % 60, seconds % 60);
    if (digits > 0)
        result += fmt::format(".{:0{}}", fraction, digits);
    return result;
}

ASTPtr typedNull(QType type)
{
    return makeASTFunction("CAST", makeLiteral(Field()), makeLiteral(Field(fmt::format("Nullable({})", qTypeToClickHouseType(type)))));
}

ASTPtr atomToAST(QType type, const QAtom & atom, size_t offset)
{
    if (atom.is_null)
    {
        if (type == QType::Symbol || type == QType::Char)
            return makeLiteral(Field(String()));
        return typedNull(type);
    }

    if (atom.infinity)
    {
        switch (type)
        {
            case QType::Float:
                return makeLiteral(Field(atom.infinity * std::numeric_limits<Float64>::infinity()));
            case QType::Real:
                return makeASTFunction("toFloat32", makeLiteral(Field(atom.infinity * std::numeric_limits<Float64>::infinity())));
            case QType::Long:
                return makeLiteral(Field(atom.infinity * std::numeric_limits<Int64>::max()));
            case QType::Int:
                return makeASTFunction("toInt32", makeLiteral(Field(static_cast<Int64>(atom.infinity) * std::numeric_limits<Int32>::max())));
            case QType::Short:
                return makeASTFunction("toInt16", makeLiteral(Field(static_cast<Int64>(atom.infinity) * std::numeric_limits<Int16>::max())));
            default:
                throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Infinities of this type are not supported");
        }
    }

    switch (type)
    {
        case QType::Boolean:
            return makeLiteral(Field(atom.value.safeGet<Int64>() != 0));
        case QType::Byte:
            return makeASTFunction("toUInt8", makeLiteral(atom.value));
        case QType::Short:
            return makeASTFunction("toInt16", makeLiteral(atom.value));
        case QType::Int:
            return makeASTFunction("toInt32", makeLiteral(atom.value));
        case QType::Long:
        case QType::Float:
            return makeLiteral(atom.value);
        case QType::Real:
            return makeASTFunction("toFloat32", makeLiteral(atom.value));
        case QType::Char:
        case QType::Symbol:
            return makeLiteral(atom.value);
        case QType::Date:
            return makeASTFunction("toDate32", makeLiteral(Field(formatDate(atom.value.safeGet<Int64>()))));
        case QType::Month:
        {
            Int64 months = atom.value.safeGet<Int64>();
            Int64 year = 2000 + floorDiv(months, 12);
            Int64 month = months - floorDiv(months, 12) * 12 + 1;
            return makeASTFunction("toDate32", makeLiteral(Field(fmt::format("{:04}-{:02}-01", year, month))));
        }
        case QType::Timestamp:
        case QType::Datetime:
        {
            /// Nanoseconds or milliseconds since 2000.01.01, each formatted in its own unit.
            bool is_timestamp = type == QType::Timestamp;
            int digits = is_timestamp ? 9 : 3;
            Int64 days = 0;
            Int64 clock = 0;
            splitDays(atom.value.safeGet<Int64>(), is_timestamp ? nanoseconds_in_day : milliseconds_in_day, days, clock);
            String text = formatDate(days) + " " + formatClock(clock, digits);
            return makeASTFunction("toDateTime64", makeLiteral(Field(text)), makeLiteral(Field(static_cast<UInt64>(digits))));
        }
        case QType::Timespan:
            return makeASTFunction("toTime64", makeLiteral(Field(formatClock(atom.value.safeGet<Int64>(), 9))), makeLiteral(Field(static_cast<UInt64>(9))));
        case QType::Time:
            return makeASTFunction("toTime64", makeLiteral(Field(formatClock(atom.value.safeGet<Int64>(), 3))), makeLiteral(Field(static_cast<UInt64>(3))));
        case QType::Minute:
        {
            /// The parser keeps the minutes within 32 bits, so the seconds fit.
            Int64 seconds = 0;
            if (common::mulOverflow(atom.value.safeGet<Int64>(), static_cast<Int64>(60), seconds))
                throwQError(ErrorCodes::BAD_ARGUMENTS, offset, "The literal of minutes is out of range");
            return makeASTFunction("toTime", makeLiteral(Field(formatClock(seconds, 0))));
        }
        case QType::Second:
            return makeASTFunction("toTime", makeLiteral(Field(formatClock(atom.value.safeGet<Int64>(), 0))));
        case QType::Guid:
        case QType::Mixed:
            break;
    }
    throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Literals of this type are not supported");
}

/// The column of each row, for the columns of a table.
QValue columnValue(const NameAndTypePair & column, const QTypes & qtypes)
{
    QValue value;
    value.kind = QValueKind::Column;
    value.ast = makeIdentifier(column.name);
    value.depth = arrayDepth(column.type, value.is_string);
    if (value.depth > 0)
        value.is_string = false;
    if (auto it = qtypes.find(column.name); it != qtypes.end())
        value.qtype = it->second;
    else
        value.qtype = dataTypeToQType(column.type);
    return value;
}

/// The q types that the types of the columns do not tell: a month and a minute.
QTypes untoldQTypes(const NamesAndTypesList & columns, const QTypes & qtypes)
{
    QTypes result;
    for (const auto & column : columns)
    {
        auto it = qtypes.find(column.name);
        if (it == qtypes.end())
            continue;
        auto told = dataTypeToQType(column.type);
        if ((told == QType::Date && it->second == QType::Month) || (told == QType::Second && it->second == QType::Minute))
            result.emplace(column.name, it->second);
    }
    return result;
}

bool isAggregateFunctionName(const String & name)
{
    return AggregateFunctionFactory::instance().isAggregateFunctionName(name);
}

/// Makes the aggregate functions of an expression the window functions over a partition: an aggregate as a column,
/// such as `update m: avg x by g from t`.
///
/// With `ordered`, the window is ordered by the number of the row (`__qrow`), and its frame is the whole partition.
/// The values are the same, but the rows keep their order: ClickHouse sorts the rows by the keys of the partition,
/// and the result is sorted back by `__qrow`, which is the number of the row before that sorting only if the sorting
/// needs it (a function that a sorting does not need is calculated after it). And the aggregates that depend on
/// the order of the rows, such as `first`, see the rows in their order.
void windowizeImpl(ASTPtr & ast, const ASTs & partition, bool ordered)
{
    checkStackSize();
    auto * function = ast->as<ASTFunction>();
    if (!function)
        return;
    if (function->isWindowFunction() || isASTLambdaFunction(*function))
        return;

    if (isAggregateFunctionName(function->name))
    {
        auto definition = make_intrusive<ASTWindowDefinition>();
        if (!partition.empty())
        {
            ASTs keys;
            for (const auto & key : partition)
                keys.push_back(cloneForQuery(key));
            definition->partition_by = makeExpressionList(std::move(keys));
            definition->children.push_back(definition->partition_by);
        }
        if (ordered)
        {
            auto order = make_intrusive<ASTOrderByElement>();
            order->direction = 1;
            order->nulls_direction = 1;
            order->children.push_back(makeIdentifier("__qrow"));
            definition->order_by = makeExpressionList({order});
            definition->children.push_back(definition->order_by);

            definition->frame_is_default = false;
            definition->frame_type = WindowFrame::FrameType::ROWS;
            definition->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
            definition->frame_begin_preceding = true;
            definition->frame_end_type = WindowFrame::BoundaryType::Unbounded;
            definition->frame_end_preceding = false;
        }
        function->setIsWindowFunction(true);
        function->window_definition = definition;
        function->children.push_back(definition);
        return;
    }

    if (function->arguments)
        for (auto & argument : function->arguments->children)
            windowizeImpl(argument, partition, ordered);
}

/// The names in `names` made unique by suffixes, as q does for the columns of a result: `x`, `x1`, `x2`.
void makeNamesUnique(Names & names)
{
    std::unordered_set<String> seen;
    for (auto & name : names)
    {
        if (seen.insert(name).second)
            continue;
        for (size_t i = 1;; ++i)
        {
            String candidate = name + std::to_string(i);
            if (seen.insert(candidate).second)
            {
                name = candidate;
                break;
            }
        }
    }
}

const QNode * functionNode(const QNode & apply)
{
    return apply.children.empty() ? nullptr : apply.children[0].get();
}

}

String deriveColumnName(const QNodePtr & node)
{
    checkStackSize();
    if (!node)
        return "x";
    switch (node->kind)
    {
        case QNodeKind::Name:
        {
            String name = node->name;
            if (!name.empty() && name[0] != '.')
            {
                size_t dot = name.rfind('.');
                if (dot != String::npos)
                    name = name.substr(dot + 1);
            }
            if (name == "i" || name.empty() || name[0] == '.')
                return "x";
            return name;
        }
        case QNodeKind::Assign:
            return node->name;
        case QNodeKind::Apply:
        {
            const QNode * function = functionNode(*node);
            if (node->children.size() < 2)
                return "x";
            if (function && function->kind == QNodeKind::Verb)
            {
                /// A glyph names by its leftmost argument, except those whose left argument is a parameter.
                bool names_right = function->name.size() == 1 && std::string_view("#_^$@.").contains(function->name[0]);
                if (node->infix && names_right)
                    return deriveColumnName(node->children.back());
                return deriveColumnName(node->children[1]);
            }
            /// A keyword or another function names by its last argument.
            return deriveColumnName(node->children.back());
        }
        default:
            return "x";
    }
}

namespace
{

/// The helper columns, aliases, and lambda parameters of the generated query are named with the prefix `__q`
/// (`__q1`, `__qrow`, `__qmatch`, ...). A q name cannot start with an underscore, but a column of a ClickHouse table,
/// of `.ch.sql`, or a column named by a symbol can. Such a column could be confused with a helper, so it is rejected:
/// every table that the q program works with comes through here.
void checkNoReservedNames(const NamesAndTypesList & columns)
{
    for (const auto & column : columns)
    {
        if (column.name.starts_with("__q"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The column '{}' cannot be used in the q dialect: the names starting with '__q' are reserved for the generated query",
                column.name);
    }
}

}

QCompiler::QCompiler(ContextPtr context_)
    : context(std::move(context_))
    , max_expanded_elements(context->getSettingsRef()[Setting::max_expanded_ast_elements])
    , max_list_depth(context->getSettingsRef()[Setting::max_parser_depth])
{
}

void QCompiler::checkListDepth(const QValue & value) const
{
    if (max_list_depth && value.depth > max_list_depth)
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
            "A list in the q program is nested deeper than {} levels (the setting 'max_parser_depth')", max_list_depth);
}

void QCompiler::checkExpandedSize(const ASTPtr & ast)
{
    if (!max_expanded_elements || !ast || expanded_sizes.contains(ast.get()))
        return;

    /// The size of the AST as a tree: an element is counted once per every reference to it. The ASTs of the values
    /// that are already counted are not walked again, and the count stops at the limit.
    size_t size = 0;
    std::vector<const IAST *> pending{ast.get()};
    while (!pending.empty())
    {
        const IAST * node = pending.back();
        pending.pop_back();
        if (auto it = expanded_sizes.find(node); it != expanded_sizes.end())
        {
            size += it->second.second;
        }
        else
        {
            ++size;
            for (const auto & child : node->children)
                if (child)
                    pending.push_back(child.get());
        }
        if (size > max_expanded_elements)
            throwTooBigExpansion(max_expanded_elements);
    }
    expanded_sizes.emplace(ast.get(), std::pair(ast, size));
}

void QCompiler::markNotNull(const ASTPtr & ast)
{
    if (ast)
        not_null_asts.emplace(ast.get(), ast);
}

bool QCompiler::isNotNull(const ASTPtr & ast) const
{
    return ast && not_null_asts.contains(ast.get());
}

void QCompiler::checkExpandedSize(const QValue & value)
{
    checkExpandedSize(value.ast);
    if (value.table)
        checkExpandedSize(value.table->table_expression);
}

String QCompiler::newName()
{
    return fmt::format("__q{}", ++name_counter);
}

String QCompiler::newLambdaParameter()
{
    return fmt::format("__qx{}", ++name_counter);
}

QValue QCompiler::scalar(ASTPtr ast, size_t depth)
{
    QValue value;
    value.kind = QValueKind::Scalar;
    value.ast = std::move(ast);
    value.depth = depth;
    return value;
}

QValue QCompiler::makeTable(ASTPtr select_query, Names keys, const QTypes & qtypes)
{
    /// The query is analyzed for its structure right here, so it is checked first.
    checkExpandedSize(select_query);
    auto header = InterpreterSelectQueryAnalyzer::getSampleBlock(select_query, context);
    auto table = std::make_shared<QTable>();
    table->table_expression = makeSubqueryTableExpression(select_query);
    table->columns = header->getNamesAndTypesList();
    checkNoReservedNames(table->columns);
    table->keys = std::move(keys);
    table->qtypes = untoldQTypes(table->columns, qtypes);

    QValue value;
    value.kind = QValueKind::Table;
    value.table = std::move(table);
    return value;
}

QValue QCompiler::withQTypes(const QValue & table, const QTypes & qtypes)
{
    QTypes merged = table.table->qtypes;
    for (const auto & [name, type] : untoldQTypes(table.table->columns, qtypes))
        merged.emplace(name, type);
    if (merged == table.table->qtypes)
        return table;

    auto copy = std::make_shared<QTable>(*table.table);
    copy->qtypes = std::move(merged);
    QValue result = table;
    result.table = std::move(copy);
    return result;
}

QValue QCompiler::tableFromSelect(ASTPtr select_query, Names keys)
{
    return makeTable(std::move(select_query), std::move(keys));
}

ASTPtr QCompiler::selectAll(const QTable & table)
{
    QSelectBuilder builder;
    builder.from = table.table_expression;
    for (const auto & column : table.columns)
        builder.select.push_back(makeIdentifier(column.name));
    return builder.build();
}

ASTPtr QCompiler::collectColumn(ASTPtr column)
{
    /// A tuple is never a `NULL`, so every row gets into the array, and the values are taken out of the tuples.
    String item = newLambdaParameter();
    return makeASTFunction("arrayMap",
        makeLambda({item}, makeASTFunction("tupleElement", makeIdentifier(item), makeLiteral(Field(static_cast<UInt64>(1))))),
        makeASTFunction("groupArray", makeASTFunction("tuple", std::move(column))));
}

ASTPtr QCompiler::pickFromColumn(const String & aggregate, ASTPtr column, ASTPtr by)
{
    ASTPtr wrapped = makeASTFunction("tuple", std::move(column));
    ASTPtr picked = by ? makeASTFunction(aggregate, std::move(wrapped), std::move(by)) : makeASTFunction(aggregate, std::move(wrapped));
    return makeASTFunction("tupleElement", std::move(picked), makeLiteral(Field(static_cast<UInt64>(1))));
}

QValue QCompiler::toScalar(const QValue & value, size_t offset)
{
    if (value.kind == QValueKind::Scalar || value.kind == QValueKind::Column)
        return value;
    if (value.kind == QValueKind::Table)
    {
        if (value.table->columns.size() != 1)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "A table with more than one column cannot be used as a list");
        const auto & column = value.table->columns.front();
        QSelectBuilder builder;
        builder.from = value.table->table_expression;
        builder.select.push_back(collectColumn(makeIdentifier(column.name)));
        bool is_string = false;
        size_t depth = arrayDepth(column.type, is_string);
        QValue result = scalar(make_intrusive<ASTSubquery>(builder.build()), depth + 1);
        result.is_string = depth == 0 && is_string;
        result.qtype = columnValue(column, value.table->qtypes).qtype;
        return result;
    }
    throwQError(ErrorCodes::TYPE_MISMATCH, offset, "A function cannot be used as a value here");
}

ASTPtr QCompiler::windowize(const ASTPtr & ast, const ASTs & partition, QScope * scope)
{
    /// Without a scope there is no number of the row, and the caller keeps the order of the rows itself.
    ASTPtr result = cloneForQuery(ast);
    windowizeImpl(result, partition, scope != nullptr);
    if (scope)
        scope->uses_row_number = true;
    return result;
}

QValue QCompiler::broadcast(const QValue & value, QScope * scope)
{
    if (value.kind != QValueKind::Scalar || !value.aggregated)
        return value;
    if (!scope)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregates are supported only in q-sql templates");
    QValue result = value;
    result.kind = QValueKind::Column;
    result.aggregated = false;
    result.windowed = true;
    result.ast = windowize(value.ast, scope->partition, scope);
    return result;
}

ASTPtr QCompiler::makeWindowFunction(const String & function, ASTs args, QScope * scope, std::optional<UInt64> preceding, UInt64 following)
{
    if (!scope)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Uniform functions over columns are supported only in q-sql templates");
    scope->uses_row_number = true;

    auto call = make_intrusive<ASTFunction>();
    call->name = function;
    call->arguments = makeExpressionList(std::move(args));
    call->children.push_back(call->arguments);

    auto definition = make_intrusive<ASTWindowDefinition>();
    if (!scope->partition.empty())
    {
        ASTs keys;
        for (const auto & key : scope->partition)
            keys.push_back(cloneForQuery(key));
        definition->partition_by = makeExpressionList(std::move(keys));
        definition->children.push_back(definition->partition_by);
    }

    auto order = make_intrusive<ASTOrderByElement>();
    order->direction = 1;
    order->nulls_direction = 1;
    order->children.push_back(makeIdentifier("__qrow"));
    definition->order_by = makeExpressionList({order});
    definition->children.push_back(definition->order_by);

    definition->frame_is_default = false;
    definition->frame_type = WindowFrame::FrameType::ROWS;
    if (preceding)
    {
        if (*preceding == 0)
            definition->frame_begin_type = WindowFrame::BoundaryType::Current;
        else
        {
            definition->frame_begin_type = WindowFrame::BoundaryType::Offset;
            definition->frame_begin_offset = makeLiteral(Field(*preceding));
            definition->children.push_back(definition->frame_begin_offset);
        }
    }
    else
        definition->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
    definition->frame_begin_preceding = true;

    if (following == 0)
        definition->frame_end_type = WindowFrame::BoundaryType::Current;
    else
    {
        definition->frame_end_type = WindowFrame::BoundaryType::Offset;
        definition->frame_end_offset = makeLiteral(Field(following));
        definition->children.push_back(definition->frame_end_offset);
    }
    definition->frame_end_preceding = false;

    call->setIsWindowFunction(true);
    call->window_definition = definition;
    call->children.push_back(definition);
    return call;
}

ASTPtr QCompiler::columnForAggregation(const QValue & column, QScope * scope)
{
    if (!column.windowed)
        return column.ast;
    if (!scope || !scope->pre_aggregation)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregating a uniform function is not supported here");
    String name = newName();
    scope->pre_aggregation->emplace_back(column.ast, name);
    return makeIdentifier(name);
}

QValue QCompiler::atomic(const String & q_name, QValues args, const std::function<ASTPtr(const ASTs &)> & make, QScope * scope)
{
    bool has_column = false;
    for (const auto & arg : args)
    {
        if (arg.kind == QValueKind::Table || arg.kind == QValueKind::Function || arg.kind == QValueKind::Dictionary)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Invalid argument of '{}' in the q program: a table, a function, or a dictionary", q_name);
        has_column |= arg.kind == QValueKind::Column;
    }

    if (has_column)
    {
        for (auto & arg : args)
        {
            if (arg.kind == QValueKind::Scalar && arg.aggregated)
                arg = broadcast(arg, scope);
            else if (arg.kind == QValueKind::Scalar && arg.depth > 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "A list cannot be combined with a column in '{}' in the q program (use 'each' or 'in')", q_name);
        }
    }

    QValue result;
    result.kind = has_column ? QValueKind::Column : QValueKind::Scalar;
    for (const auto & arg : args)
    {
        result.aggregated |= arg.aggregated;
        result.windowed |= arg.windowed;
        result.depth = std::max(result.depth, arg.depth);
    }

    /// Recursively maps the function over the atoms of the lists, broadcasting the atoms.
    std::function<ASTPtr(ASTs, std::vector<size_t>)> map = [&](ASTs asts, std::vector<size_t> depths) -> ASTPtr
    {
        /// One call for every level of the lists: see `checkListDepth`.
        checkStackSize();

        bool any_list = false;
        for (size_t depth : depths)
            any_list |= depth > 0;
        if (!any_list)
            return make(asts);

        Names params;
        ASTs mapped;
        for (size_t i = 0; i < asts.size(); ++i)
        {
            if (depths[i] > 0)
            {
                String param = newLambdaParameter();
                params.push_back(param);
                mapped.push_back(asts[i]);
                asts[i] = makeIdentifier(param);
                --depths[i];
            }
        }
        ASTPtr body = map(asts, depths);
        auto call = makeASTFunction("arrayMap", makeLambda(params, body));
        for (auto & array : mapped)
            call->arguments->children.push_back(array);
        return call;
    };

    ASTs asts;
    std::vector<size_t> depths;
    for (const auto & arg : args)
    {
        asts.push_back(arg.ast);
        depths.push_back(arg.depth);
    }
    result.ast = map(asts, depths);
    return result;
}

std::optional<QValue> QCompiler::resolveTable(const String & name)
{
    String database;
    String table = name;
    if (size_t dot = name.find('.'); dot != String::npos && dot > 0)
    {
        database = name.substr(0, dot);
        table = name.substr(dot + 1);
    }

    StorageID id = context->tryResolveStorageID(StorageID(database, table));
    if (!id)
        return std::nullopt;
    if (!DatabaseCatalog::instance().isTableExist(id, context))
        return std::nullopt;

    /// A temporary table is referred to by its name alone, as it is written in the program: its storage lives in the
    /// internal database of the temporary tables under a generated name, which a query is not allowed to use directly.
    /// The generated query resolves the name within the session again, as a query in SQL does.
    bool temporary = database.empty() && id.database_name == DatabaseCatalog::TEMPORARY_DATABASE;
    auto table_expression = temporary ? makeTableIdentifierExpression(String(), table)
                                      : makeTableIdentifierExpression(id.database_name, id.table_name);

    /// The structure is taken from the metadata of the table rather than from analyzing `SELECT * FROM t`,
    /// which would require the `SELECT` grant on every column, even if the q program uses only some of them
    /// (or none, like `cols t`). The columns are those that the user is allowed to see, as in `system.columns`.
    /// The generated query is checked for access as usual when it is executed.
    StoragePtr storage = DatabaseCatalog::instance().getTable(id, context);
    auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    NamesAndTypesList columns = metadata->getColumns().getOrdinary();
    if (id.hasDatabase() && id.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        auto access = context->getAccess();
        if (!access->isGranted(AccessType::SHOW_COLUMNS, id.database_name, id.table_name))
        {
            std::erase_if(columns, [&](const NameAndTypePair & column)
            {
                return !access->isGranted(AccessType::SHOW_COLUMNS, id.database_name, id.table_name, column.name);
            });
            /// Nothing of the table is visible to the user: report it as the lack of the grant.
            if (columns.empty())
                context->checkAccess(AccessType::SELECT, id);
        }
    }

    /// An `Alias` table has the metadata of its target, so its columns are also seen through the grants on every table
    /// that the alias resolves through, as in `system.columns`: a grant on the alias alone does not show them.
    if (const auto * alias = storage->as<StorageAlias>())
    {
        NameSet granted = alias->filterColumnsGrantedThroughChain(context, AccessType::SHOW_COLUMNS, columns.getNames());
        std::erase_if(columns, [&](const NameAndTypePair & column) { return !granted.contains(column.name); });
        if (columns.empty())
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Not enough privileges to see the columns of the table {} through the tables that it is an alias of", id.getNameForLogs());
    }
    checkNoReservedNames(columns);

    auto result_table = std::make_shared<QTable>();
    result_table->table_expression = table_expression;
    result_table->columns = std::move(columns);

    QValue value;
    value.kind = QValueKind::Table;
    value.table = std::move(result_table);
    return value;
}

QValue QCompiler::compileLiteral(const QNode & node)
{
    if (node.type == QType::Char)
    {
        QValue value = scalar(makeLiteral(node.atoms.front().value));
        value.is_string = true;
        value.qtype = QType::Char;
        markNotNull(value.ast);
        return value;
    }

    QValue value;
    value.qtype = node.type;
    if (!node.is_list)
    {
        value = scalar(atomToAST(node.type, node.atoms.front(), node.offset));
        value.qtype = node.type;
        /// A null symbol is an empty string, not a `NULL`.
        if (!node.atoms.front().is_null || node.type == QType::Symbol)
            markNotNull(value.ast);
        return value;
    }

    ASTs items;
    for (const auto & atom : node.atoms)
        items.push_back(atomToAST(node.type, atom, node.offset));
    auto array = make_intrusive<ASTFunction>();
    array->name = "array";
    array->arguments = makeExpressionList(std::move(items));
    array->children.push_back(array->arguments);
    value = scalar(array, 1);
    value.qtype = node.type;
    return value;
}

QValue QCompiler::compileName(const QNodePtr & node, QEnvironment & env, QScope * scope, bool function_position)
{
    const String & name = node->name;

    auto lookup_env = [&]() -> const QValue *
    {
        if (auto it = env.names.find(name); it != env.names.end())
            return &it->second;
        if (env.globals)
            if (auto it = env.globals->names.find(name); it != env.globals->names.end())
                return &it->second;
        return nullptr;
    };

    auto lookup_column = [&]() -> std::optional<QValue>
    {
        if (!scope)
            return std::nullopt;
        for (const auto & column : scope->columns)
        {
            if (column.name == name)
            {
                QValue value = columnValue(column, scope->qtypes);
                if (value.depth == 0 && !removeLowCardinality(column.type)->isNullable())
                    markNotNull(value.ast);
                return value;
            }
        }
        if (name == "i" && !scope->columns.empty())
        {
            QValue value;
            value.kind = QValueKind::Column;
            value.ast = makeIdentifier("__qrow");
            value.qtype = QType::Long;
            scope->uses_row_number = true;
            return value;
        }
        return std::nullopt;
    };

    auto builtin = [&]() -> std::optional<QValue>
    {
        if (!isBuiltin(name))
            return std::nullopt;
        auto function = std::make_shared<QFunction>();
        function->builtin = name;
        QValue value;
        value.kind = QValueKind::Function;
        value.function = std::move(function);
        return value;
    };

    if (function_position)
    {
        if (const auto * value = lookup_env())
            return *value;
        if (auto value = builtin())
            return *value;
        if (auto value = lookup_column())
            return *value;
    }
    else
    {
        if (auto value = lookup_column())
            return *value;
        if (const auto * value = lookup_env())
            return *value;
        if (auto value = builtin())
            return *value;
    }

    /// System values.
    if (name == ".z.d" || name == ".z.D")
    {
        QValue value = scalar(makeASTFunction("toDate32", makeASTFunction("today")));
        value.qtype = QType::Date;
        return value;
    }
    if (name == ".z.p" || name == ".z.P")
    {
        QValue value = scalar(makeASTFunction("now64", makeLiteral(Field(static_cast<UInt64>(9)))));
        value.qtype = QType::Timestamp;
        return value;
    }
    if (name == ".z.t" || name == ".z.T")
    {
        QValue value = scalar(makeASTFunction("toTime64", makeASTFunction("now64", makeLiteral(Field(static_cast<UInt64>(3)))), makeLiteral(Field(static_cast<UInt64>(3)))));
        value.qtype = QType::Time;
        return value;
    }

    /// Dotted names: a temporal accessor of a column (`time.hh`), a column of a table (`t.price`), or a table in a database.
    if (size_t dot = name.rfind('.'); dot != String::npos && dot > 0)
    {
        String prefix = name.substr(0, dot);
        String suffix = name.substr(dot + 1);

        if (scope)
        {
            for (const auto & column : scope->columns)
            {
                if (column.name == prefix)
                {
                    QValues args{columnValue(column, scope->qtypes)};
                    return callBuiltin("." + suffix, std::move(args), env, scope, node->offset);
                }
            }
        }

        auto prefix_node = std::make_shared<QNode>(QNodeKind::Name, node->offset);
        prefix_node->name = prefix;
        std::optional<QValue> prefix_value;
        if (env.names.contains(prefix) || (env.globals && env.globals->names.contains(prefix)))
            prefix_value = compileName(prefix_node, env, scope, false);
        if (prefix_value && prefix_value->kind == QValueKind::Table)
        {
            for (const auto & column : prefix_value->table->columns)
            {
                if (column.name == suffix)
                {
                    QSelectBuilder builder;
                    builder.from = prefix_value->table->table_expression;
                    builder.select.push_back(makeIdentifier(suffix));
                    return toScalar(makeTable(builder.build(), {}, prefix_value->table->qtypes), node->offset);
                }
            }
        }
    }

    if (auto table = resolveTable(name))
        return *table;

    throwQError(ErrorCodes::UNKNOWN_IDENTIFIER, node->offset, fmt::format("Unknown name '{}'", name));
}

QValue QCompiler::compileList(const QNodePtr & node, QEnvironment & env, QScope * scope)
{
    if (node->children.empty())
        return scalar(makeASTFunction("array"), 1);

    /// The items are evaluated from the last one to the first one, as everything in q:
    /// `x:10; (x+5; x:20; x-5)` is `25 20 5`.
    QValues items(node->children.size());
    for (size_t i = node->children.size(); i > 0; --i)
        items[i - 1] = toScalar(compile(node->children[i - 1], env, scope), node->children[i - 1]->offset);

    bool has_column = false;
    size_t depth = 0;
    bool all_strings = true;
    for (auto & item : items)
    {
        if (item.kind == QValueKind::Function || item.kind == QValueKind::Dictionary || item.kind == QValueKind::Table)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, node->offset, "General lists of functions or tables are not supported");
        has_column |= item.kind == QValueKind::Column;
        depth = std::max(depth, item.depth);
        all_strings &= item.is_string && item.depth == 0;
    }

    /// A list of columns, like `(a;b)`, is a column of arrays (q would have a list of the columns).
    QValue result;
    result.kind = has_column ? QValueKind::Column : QValueKind::Scalar;
    auto array = make_intrusive<ASTFunction>();
    array->name = "array";
    array->arguments = make_intrusive<ASTExpressionList>();
    array->children.push_back(array->arguments);
    for (auto & item : items)
    {
        if (has_column && item.kind == QValueKind::Scalar && item.aggregated)
            item = broadcast(item, scope);
        if (item.depth != depth)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, node->offset, "General lists of items of different depths are not supported");
        result.aggregated |= item.aggregated;
        result.windowed |= item.windowed;
        array->arguments->children.push_back(item.ast);
    }
    result.ast = array;
    result.depth = depth + 1;
    result.qtype = items.front().qtype;
    (void)all_strings;
    return result;
}

QValue QCompiler::compileTableLiteral(const QNode & node, QEnvironment & env, QScope * scope)
{
    Names names;
    std::vector<QValue> values;
    for (const auto * columns : {&node.key_columns, &node.columns})
    {
        for (const auto & column : *columns)
        {
            QValue value = toScalar(compile(column.expression, env, scope), column.expression->offset);
            if (value.kind != QValueKind::Scalar || value.aggregated)
                throwQError(ErrorCodes::NOT_IMPLEMENTED, column.expression->offset, "The columns of a table literal must be lists or atoms");
            names.push_back(column.name.empty() ? deriveColumnName(column.expression) : column.name);
            values.push_back(std::move(value));
        }
    }
    makeNamesUnique(names);

    QSelectBuilder inner;
    QSelectBuilder outer;
    for (size_t i = 0; i < names.size(); ++i)
    {
        inner.select.push_back(withAlias(values[i].ast, names[i]));
        outer.select.push_back(makeIdentifier(names[i]));
        /// A list becomes the rows of the column, including a list of strings (a string itself is an atom, without a depth).
        if (values[i].depth > 0)
            outer.array_join.push_back(makeIdentifier(names[i]));
    }

    Names keys;
    for (size_t i = 0; i < node.key_columns.size(); ++i)
        keys.push_back(names[i]);

    QTypes qtypes;
    for (size_t i = 0; i < names.size(); ++i)
        if (values[i].qtype)
            qtypes[names[i]] = *values[i].qtype;

    if (outer.array_join.empty())
        return makeTable(inner.build(), keys, qtypes);

    outer.from = makeSubqueryTableExpression(inner.build());
    return makeTable(outer.build(), keys, qtypes);
}

QValue QCompiler::compileLambdaCall(const QNode & lambda, QValues args, QEnvironment & env, QScope * scope)
{
    if (inline_depth >= max_inline_depth)
        throwQError(ErrorCodes::TOO_DEEP_RECURSION, lambda.offset, "Too deep nesting of function calls (recursion is not supported in the q dialect)");
    ++inline_depth;
    SCOPE_EXIT({ --inline_depth; });

    if (args.size() > lambda.params.size())
        throwQError(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, lambda.offset,
            fmt::format("The function {} takes {} arguments, but {} are given", lambda.source, lambda.params.size(), args.size()));

    QEnvironment locals;
    locals.globals = env.globals ? env.globals : &env;
    for (size_t i = 0; i < args.size(); ++i)
        locals.names[lambda.params[i]] = std::move(args[i]);

    /// The body of a function sees its parameters and the globals, but not the columns of the table.
    std::optional<QScope> body_scope;
    if (scope)
    {
        body_scope.emplace(*scope);
        body_scope->columns.clear();
        body_scope->qtypes.clear();
    }

    std::optional<QValue> result;
    for (const auto & statement : lambda.children)
    {
        if (!statement)
            continue;
        result = compile(statement, locals, body_scope ? &*body_scope : nullptr);
    }

    if (scope)
        scope->uses_row_number |= body_scope->uses_row_number;

    if (!result)
        throwQError(ErrorCodes::NOT_IMPLEMENTED, lambda.offset, "Functions without a result are not supported");
    return *result;
}

QValue QCompiler::apply(const QFunctionPtr & function, QValues args, QEnvironment & env, QScope * scope, size_t offset)
{
    /// A function is applied not only where the program says so, but also by the adverbs and by other functions.
    checkStackSize();
    QValue result = applyFunction(function, std::move(args), env, scope, offset);
    checkExpandedSize(result);
    checkListDepth(result);
    return result;
}

QValue QCompiler::applyFunction(const QFunctionPtr & function, QValues args, QEnvironment & env, QScope * scope, size_t offset)
{
    /// Fill the elided arguments of a projection.
    if (!function->bound.empty())
    {
        QValues merged;
        size_t next = 0;
        for (const auto & bound : function->bound)
        {
            if (bound)
                merged.push_back(*bound);
            else if (next < args.size())
                merged.push_back(args[next++]);     /// A copy: the arguments are used again below if a slot remains empty.
            else
            {
                /// Not all the slots are filled, like in `g:f[1]; h:g[2]`: the result is a projection again.
                auto projection = std::make_shared<QFunction>(*function);
                size_t bound_index = 0;
                for (auto & slot : projection->bound)
                {
                    if (!slot && bound_index < args.size())
                        slot = args[bound_index++];
                }
                QValue value;
                value.kind = QValueKind::Function;
                value.function = std::move(projection);
                return value;
            }
        }
        for (; next < args.size(); ++next)
            merged.push_back(std::move(args[next]));
        auto unbound = std::make_shared<QFunction>(*function);
        unbound->bound.clear();
        return apply(unbound, std::move(merged), env, scope, offset);
    }

    size_t arity = 0;
    if (function->lambda)
        arity = function->lambda->params.size();
    else if (!function->builtin.empty())
        arity = builtinArity(function->builtin);

    if (arity && args.size() < arity && !(function->lambda && args.empty()))
    {
        auto projection = std::make_shared<QFunction>(*function);
        for (auto & arg : args)
            projection->bound.emplace_back(std::move(arg));
        while (projection->bound.size() < arity)
            projection->bound.emplace_back(std::nullopt);
        QValue value;
        value.kind = QValueKind::Function;
        value.function = std::move(projection);
        return value;
    }

    if (function->lambda)
        return compileLambdaCall(*function->lambda, std::move(args), env, scope);
    if (!function->adverb.empty())
        return applyDerived(*function, std::move(args), env, scope, offset);
    return callBuiltin(function->builtin, std::move(args), env, scope, offset);
}

QValue QCompiler::applyValue(const QValue & target, QValues args, QEnvironment & env, QScope * scope, size_t offset)
{
    if (target.kind == QValueKind::Function)
        return apply(target.function, std::move(args), env, scope, offset);

    if (args.size() != 1)
        throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Indexing at depth is not supported");

    QValue index = toScalar(args[0], offset);

    if (target.kind == QValueKind::Table)
    {
        /// t`c or t[`c]: the column of a table, as a list.
        if (index.qtype == QType::Symbol && index.depth == 0)
        {
            const auto * literal = index.ast->as<ASTLiteral>();
            if (!literal)
                throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "The column of a table must be a symbol literal");
            String column = literal->value.safeGet<String>();
            QSelectBuilder builder;
            builder.from = target.table->table_expression;
            builder.select.push_back(makeIdentifier(column));
            return toScalar(makeTable(builder.build(), {}, target.table->qtypes), offset);
        }
        throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Indexing a table by row is not supported");
    }

    if (target.kind == QValueKind::Dictionary)
    {
        const auto & [keys, values] = *target.dictionary;
        QValue result = atomic("@", {index},
            [&](const ASTs & asts)
            {
                return makeASTFunction("arrayElement", values.ast, makeASTFunction("indexOf", keys.ast, asts[0]));
            },
            scope);
        result.depth += values.depth - 1;
        result.is_string = values.is_string;
        result.qtype = values.qtype;
        return result;
    }

    if (target.kind == QValueKind::Scalar && target.is_string && target.depth == 0)
    {
        QValue result = atomic("@", {index},
            [&](const ASTs & asts)
            {
                return makeASTFunction("substring", target.ast, makeASTFunction("plus", asts[0], makeLiteral(Field(static_cast<UInt64>(1)))),
                    makeLiteral(Field(static_cast<UInt64>(1))));
            },
            scope);
        result.is_string = true;
        return result;
    }

    if ((target.kind == QValueKind::Scalar || target.kind == QValueKind::Column) && target.depth > 0)
    {
        QValue list = target;
        QValue result = atomic("@", {index},
            [&](const ASTs & asts)
            {
                return makeASTFunction("arrayElement", list.ast, makeASTFunction("plus", asts[0], makeLiteral(Field(static_cast<UInt64>(1)))));
            },
            scope);
        if (target.kind == QValueKind::Column)
        {
            result.kind = QValueKind::Column;
            result.windowed |= target.windowed;
        }
        result.aggregated |= target.aggregated;
        result.depth += target.depth - 1;
        result.is_string = target.is_string;
        result.qtype = target.qtype;
        return result;
    }

    throwQError(ErrorCodes::TYPE_MISMATCH, offset, "The value cannot be applied or indexed");
}

QValue QCompiler::compileApply(const QNodePtr & node, QEnvironment & env, QScope * scope)
{
    const QNodePtr & function_node = node->children[0];

    /// `(agg; column) fby group`: an aggregate of a column by groups, as a column.
    if (function_node->kind == QNodeKind::Name && function_node->name == "fby" && node->children.size() == 3)
    {
        const QNodePtr & pair = node->children[1];
        if (!pair || pair->kind != QNodeKind::List || pair->children.size() != 2)
            throwQError(ErrorCodes::SYNTAX_ERROR, node->offset, "The left argument of 'fby' must be a pair (aggregate; column)");
        if (!scope)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, node->offset, "'fby' is supported only in q-sql templates");

        QValue group = compile(node->children[2], env, scope);
        QValue column = compile(pair->children[1], env, scope);
        QValue aggregate = compile(pair->children[0], env, scope);
        if (group.kind != QValueKind::Column)
            throwQError(ErrorCodes::TYPE_MISMATCH, node->offset, "The right argument of 'fby' must be a column");

        QScope group_scope = *scope;
        group_scope.partition = {group.ast};
        QValue folded = applyValue(aggregate, {column}, env, &group_scope, node->offset);
        scope->uses_row_number |= group_scope.uses_row_number;
        if (folded.kind != QValueKind::Scalar || !folded.aggregated)
            throwQError(ErrorCodes::TYPE_MISMATCH, node->offset, "The function of 'fby' must be an aggregate");
        QValue result = broadcast(folded, &group_scope);
        scope->uses_row_number = true;
        return result;
    }

    /// `x within (low; high)`: the bounds are compiled separately, as they can be columns.
    if (function_node->kind == QNodeKind::Name && function_node->name == "within" && node->children.size() == 3
        && node->children[2] && node->children[2]->kind == QNodeKind::List && node->children[2]->children.size() == 2)
    {
        QValue high = compile(node->children[2]->children[1], env, scope);
        QValue low = compile(node->children[2]->children[0], env, scope);
        QValue value = compile(node->children[1], env, scope);
        QValues args{std::move(value), std::move(low), std::move(high)};
        return atomic("within", std::move(args),
            [](const ASTs & asts)
            {
                return makeASTFunction("and", makeASTFunction("greaterOrEquals", asts[0], asts[1]), makeASTFunction("lessOrEquals", asts[0], asts[2]));
            },
            scope);
    }

    /// q evaluates from right to left: the arguments from the last one to the first one, and then the function.
    /// It matters when an argument assigns a variable that another one reads: `a:0; a+(a:2)` is 4.
    QValues args(node->children.size() - 1);
    std::vector<bool> elided(args.size(), false);
    bool any_elided = false;
    for (size_t i = args.size(); i > 0; --i)
    {
        const auto & child = node->children[i];
        if (child)
        {
            args[i - 1] = compile(child, env, scope);
        }
        else
        {
            elided[i - 1] = true;
            any_elided = true;
        }
    }

    QValue function;
    if (function_node->kind == QNodeKind::Name)
        function = compileName(function_node, env, scope, true);
    else
        function = compile(function_node, env, scope);

    /// `f[]` calls a function without arguments.
    if (args.size() == 1 && elided[0])
    {
        if (function.kind == QValueKind::Function && function.function->lambda)
            return compileLambdaCall(*function.function->lambda, {}, env, scope);
        any_elided = true;
    }

    if (any_elided)
    {
        if (function.kind != QValueKind::Function)
            throwQError(ErrorCodes::SYNTAX_ERROR, node->offset, "Elided arguments are allowed only for functions");
        auto projection = std::make_shared<QFunction>(*function.function);
        if (projection->bound.empty())
        {
            for (size_t i = 0; i < args.size(); ++i)
                projection->bound.push_back(elided[i] ? std::nullopt : std::optional<QValue>(args[i]));
        }
        else
        {
            /// The function is a projection already, like `g` in `g:f[1]; h:g[;3]`. Its arguments stay fixed,
            /// and the new ones go to its empty slots in their order: an elided one leaves its slot empty.
            size_t next = 0;
            for (auto & slot : projection->bound)
            {
                if (slot)
                    continue;
                if (next == args.size())
                    break;
                if (!elided[next])
                    slot = args[next];
                ++next;
            }
            for (; next < args.size(); ++next)
                projection->bound.push_back(elided[next] ? std::nullopt : std::optional<QValue>(args[next]));
        }
        QValue value;
        value.kind = QValueKind::Function;
        value.function = std::move(projection);
        return value;
    }

    return applyValue(function, std::move(args), env, scope, node->offset);
}

QValue QCompiler::compile(const QNodePtr & node, QEnvironment & env, QScope * scope)
{
    /// The tree of a q program can be as deep as `max_parser_depth`, and every level takes several frames here.
    checkStackSize();
    QValue result = compileNode(node, env, scope);
    checkExpandedSize(result);
    checkListDepth(result);
    return result;
}

QValue QCompiler::compileNode(const QNodePtr & node, QEnvironment & env, QScope * scope)
{
    if (!node)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty expression in the q program");

    switch (node->kind)
    {
        case QNodeKind::Literal:
            return compileLiteral(*node);
        case QNodeKind::Name:
            return compileName(node, env, scope, false);
        case QNodeKind::Verb:
        {
            auto function = std::make_shared<QFunction>();
            function->builtin = node->name;
            QValue value;
            value.kind = QValueKind::Function;
            value.function = std::move(function);
            return value;
        }
        case QNodeKind::Derived:
        {
            QValue operand = node->children[0]->kind == QNodeKind::Name ? compileName(node->children[0], env, scope, true)
                                                                          : compile(node->children[0], env, scope);
            if (operand.kind != QValueKind::Function)
                throwQError(ErrorCodes::NOT_IMPLEMENTED, node->offset, fmt::format("The adverb '{}' applies only to functions", node->name));
            auto function = std::make_shared<QFunction>();
            function->adverb = node->name;
            function->operand = operand.function;
            QValue value;
            value.kind = QValueKind::Function;
            value.function = std::move(function);
            return value;
        }
        case QNodeKind::Apply:
            return compileApply(node, env, scope);
        case QNodeKind::List:
            return compileList(node, env, scope);
        case QNodeKind::Lambda:
        {
            auto function = std::make_shared<QFunction>();
            function->lambda = node;
            QValue value;
            value.kind = QValueKind::Function;
            value.function = std::move(function);
            return value;
        }
        case QNodeKind::Assign:
        {
            QValue value = compile(node->children[0], env, scope);
            if (value.kind == QValueKind::Column)
                throwQError(ErrorCodes::NOT_IMPLEMENTED, node->offset, "Assigning a column to a variable is not supported");
            env.names[node->name] = value;
            return value;
        }
        case QNodeKind::SQL:
            return compileSQL(*node->sql, env, scope, node->offset);
        case QNodeKind::Table:
            return compileTableLiteral(*node, env, scope);
    }
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Unknown node in the q program");
}

QValue QCompiler::applyDerived(const QFunction & function, QValues args, QEnvironment & env, QScope * scope, size_t offset)
{
    const String & adverb = function.adverb;
    const QFunctionPtr & operand = function.operand;
    String glyph = operand->builtin;

    auto apply_operand = [&](QValues operand_args) { return apply(operand, std::move(operand_args), env, scope, offset); };

    /// A derived function has no arity of its own, so it can be applied to an empty list of arguments: `f:+/; f . ()`.
    if (args.empty())
        throwQError(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, offset,
            fmt::format("The function modified by the adverb '{}' is applied without arguments", adverb));

    /// Each: `f'[x;y]`, `f each x`. Each-right: `x f/: y`. Each-left: `x f\: y`.
    if (adverb == "'" || adverb == "/:" || adverb == "\\:")
    {
        std::vector<bool> mapped(args.size(), adverb == "'");
        if (adverb == "/:")
        {
            if (args.size() != 2)
                throwQError(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, offset, "Each-right takes two arguments");
            mapped[1] = true;
        }
        if (adverb == "\\:")
        {
            if (args.size() != 2)
                throwQError(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, offset, "Each-left takes two arguments");
            mapped[0] = true;
        }

        for (auto & arg : args)
            arg = toScalar(arg, offset);

        bool any_column = false;
        bool any_list = false;
        for (size_t i = 0; i < args.size(); ++i)
        {
            if (!mapped[i])
                continue;
            any_column |= args[i].kind == QValueKind::Column;
            any_list |= args[i].kind == QValueKind::Scalar && args[i].depth > 0;
        }

        if (any_column)
        {
            /// Row by row: the function is applied to the value of each row.
            QValues row_args;
            bool windowed = false;
            for (auto & arg : args)
            {
                QValue row = arg.kind == QValueKind::Scalar && arg.aggregated ? broadcast(arg, scope) : arg;
                windowed |= row.windowed;
                row.kind = QValueKind::Scalar;
                row.windowed = false;
                row_args.push_back(std::move(row));
            }
            QValue result = apply_operand(std::move(row_args));
            if (result.kind != QValueKind::Scalar || result.aggregated)
                throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "The function applied to each row must return an atom or a list");
            result.kind = QValueKind::Column;
            result.windowed = windowed;
            return result;
        }

        if (!any_list)
            return apply_operand(std::move(args));

        Names params;
        ASTs arrays;
        QValues element_args;
        bool aggregated = false;
        for (size_t i = 0; i < args.size(); ++i)
        {
            aggregated |= args[i].aggregated;
            if (mapped[i] && args[i].depth > 0)
            {
                String param = newLambdaParameter();
                params.push_back(param);
                arrays.push_back(args[i].ast);
                QValue element = args[i];
                element.ast = makeIdentifier(param);
                element.depth -= 1;
                element.aggregated = false;
                element_args.push_back(std::move(element));
            }
            else
            {
                QValue element = args[i];
                element_args.push_back(std::move(element));
            }
        }
        QValue body = apply_operand(std::move(element_args));
        if (body.kind != QValueKind::Scalar)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "The function applied to each item must return an atom or a list");
        auto call = makeASTFunction("arrayMap", makeLambda(params, body.ast));
        for (auto & array : arrays)
            call->arguments->children.push_back(array);
        QValue result = scalar(call, body.depth + 1);
        result.aggregated = aggregated || body.aggregated;
        result.qtype = body.qtype;
        result.is_string = false;
        return result;
    }

    /// Each-prior: `f': x` applies `f` to each item and its predecessor.
    if (adverb == "':")
    {
        if (args.size() != 1)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Each-prior with a seed is not supported");
        if (glyph == "-")
            return callBuiltin("deltas", std::move(args), env, scope, offset);
        if (glyph == "%")
            return callBuiltin("ratios", std::move(args), env, scope, offset);
        QValue prior = callBuiltin("prev", args, env, scope, offset);
        return apply_operand({args[0], prior});
    }

    /// Over and scan.
    if (adverb == "/" || adverb == "\\")
    {
        bool over = adverb == "/";
        if (args.size() == 1)
        {
            static const std::map<String, std::pair<String, String>> folds = {
                {"+", {"sum", "sums"}},
                {"*", {"prd", "prds"}},
                {"|", {"max", "maxs"}},
                {"&", {"min", "mins"}},
                {",", {"raze", ""}},
            };
            if (auto it = folds.find(glyph); it != folds.end())
            {
                const String & builtin = over ? it->second.first : it->second.second;
                if (!builtin.empty())
                    return callBuiltin(builtin, std::move(args), env, scope, offset);
            }
        }

        if (args.size() > 2)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Over and scan with more than two arguments are not supported");

        QValue list = toScalar(args.back(), offset);
        if (list.kind != QValueKind::Scalar || list.depth == 0)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "Over and scan of this function are supported only for lists");

        String accumulator = newLambdaParameter();
        String item = newLambdaParameter();
        QValue element = list;
        element.ast = makeIdentifier(item);
        element.depth -= 1;
        element.aggregated = false;

        ASTPtr initial;
        ASTPtr items;
        QValue seed_value;
        if (args.size() == 2)
        {
            seed_value = toScalar(args[0], offset);
            initial = seed_value.ast;
            items = list.ast;
        }
        else
        {
            seed_value = element;
            initial = makeASTFunction("arrayElement", list.ast, makeLiteral(Field(static_cast<UInt64>(1))));
            items = makeASTFunction("arrayPopFront", list.ast);
        }

        /// The accumulator of `arrayFold` must keep its type, so numbers are accumulated as `Int64` or `Float64`.
        std::optional<QType> numeric_type = seed_value.qtype ? seed_value.qtype : list.qtype;
        String widen;
        if (numeric_type == QType::Boolean || numeric_type == QType::Byte || numeric_type == QType::Short || numeric_type == QType::Int
            || numeric_type == QType::Long)
            widen = "toInt64";
        else if (numeric_type == QType::Real || numeric_type == QType::Float)
            widen = "toFloat64";
        auto widened = [&](ASTPtr ast) -> ASTPtr { return widen.empty() ? ast : ASTPtr(makeASTFunction(widen, std::move(ast))); };
        initial = widened(initial);

        QValue acc = seed_value;
        acc.aggregated = false;
        if (over)
        {
            acc.ast = makeIdentifier(accumulator);
            QValue step = apply_operand({acc, element});
            if (step.depth == 0)
                step.ast = widened(step.ast);
            QValue result = scalar(makeASTFunction("arrayFold", makeLambda({accumulator, item}, step.ast), items, initial), step.depth);
            result.aggregated = list.aggregated;
            result.qtype = step.qtype;
            return result;
        }

        acc.ast = makeASTFunction("arrayElement", makeIdentifier(accumulator), makeLiteral(Field(static_cast<Int64>(-1))));
        QValue step = apply_operand({acc, element});
        if (step.depth != seed_value.depth)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "A scan with a function that changes the shape of the items is not supported");
        ASTPtr step_ast = makeASTFunction("arrayPushBack", makeIdentifier(accumulator), widened(step.ast));
        ASTPtr result_ast = makeASTFunction("arrayFold", makeLambda({accumulator, item}, step_ast), items, makeASTFunction("array", initial));
        if (args.size() == 2)
            result_ast = makeASTFunction("arrayPopFront", result_ast);
        QValue result = scalar(result_ast, step.depth + 1);
        result.aggregated = list.aggregated;
        result.qtype = step.qtype;
        return result;
    }

    throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, fmt::format("The adverb '{}' is not supported", adverb));
}

namespace
{

ASTPtr conjunction(const ASTs & conditions)
{
    if (conditions.size() == 1)
        return conditions[0];
    auto result = make_intrusive<ASTFunction>();
    result->name = "and";
    result->arguments = makeExpressionList(conditions);
    result->children.push_back(result->arguments);
    return result;
}

/// The layers under the main select of a q-sql template: the numbering of the rows and the columns computed before the aggregation.
ASTPtr buildSource(const QTable & source, const QScope & scope, const std::vector<std::pair<ASTPtr, String>> & pre_aggregation)
{
    ASTPtr table_expression = source.table_expression;
    if (scope.uses_row_number)
    {
        QSelectBuilder numbered;
        numbered.from = table_expression;
        numbered.select.push_back(make_intrusive<ASTAsterisk>());
        numbered.select.push_back(withAlias(makeASTFunction("rowNumberInAllBlocks"), "__qrow"));
        table_expression = makeSubqueryTableExpression(numbered.build());
    }
    if (!pre_aggregation.empty())
    {
        QSelectBuilder pre;
        pre.from = table_expression;
        pre.select.push_back(make_intrusive<ASTAsterisk>());
        for (const auto & [expression, name] : pre_aggregation)
            pre.select.push_back(withAlias(expression, name));
        if (scope.uses_row_number)
            pre.order_by.emplace_back(makeIdentifier("__qrow"), false);
        table_expression = makeSubqueryTableExpression(pre.build());
    }
    return table_expression;
}

bool hasWindow(const ASTPtr & ast)
{
    checkStackSize();
    if (const auto * function = ast->as<ASTFunction>())
    {
        if (function->isWindowFunction())
            return true;
        if (function->arguments)
            for (const auto & argument : function->arguments->children)
                if (hasWindow(argument))
                    return true;
    }
    return false;
}

}

QTable QCompiler::applyWhere(const QTable & source, const std::vector<QNodePtr> & where, QEnvironment & env)
{
    QTable current = source;
    ASTs pending;

    auto apply_pending = [&]()
    {
        if (pending.empty())
            return;
        QSelectBuilder builder;
        builder.from = current.table_expression;
        for (const auto & column : current.columns)
            builder.select.push_back(makeIdentifier(column.name));
        builder.where = conjunction(pending);
        Names keys = current.keys;
        QValue table = makeTable(builder.build(), keys, current.qtypes);
        current = *table.table;
        pending.clear();
    };

    for (const auto & constraint : where)
    {
        auto compile_constraint = [&](std::vector<std::pair<ASTPtr, String>> & pre, QScope & scope)
        {
            scope.columns = current.columns;
            scope.qtypes = current.qtypes;
            scope.pre_aggregation = &pre;
            QValue value = toScalar(compile(constraint, env, &scope), constraint->offset);
            if (value.kind == QValueKind::Scalar && value.aggregated)
                value = broadcast(value, &scope);
            if (value.kind != QValueKind::Column && value.kind != QValueKind::Scalar)
                throwQError(ErrorCodes::TYPE_MISMATCH, constraint->offset, "A constraint of the where phrase must be a boolean column");
            if (value.depth > 0)
                throwQError(ErrorCodes::TYPE_MISMATCH, constraint->offset, "A constraint of the where phrase must be a boolean column, not a list");
            return value;
        };

        std::vector<std::pair<ASTPtr, String>> pre;
        QScope scope;
        QValue value = compile_constraint(pre, scope);

        bool simple = !value.windowed && pre.empty() && !scope.uses_row_number && !hasWindow(value.ast);
        if (simple)
        {
            pending.push_back(value.ast);
            continue;
        }

        /// The constraint has aggregates or uniform functions: they apply to the rows selected by the previous constraints.
        if (!pending.empty())
        {
            apply_pending();
            pre.clear();
            scope = QScope{};
            value = compile_constraint(pre, scope);
        }

        String flag = newName();
        QSelectBuilder flagged;
        flagged.from = buildSource(current, scope, pre);
        flagged.select.push_back(make_intrusive<ASTAsterisk>());
        flagged.select.push_back(withAlias(value.ast, flag));

        QSelectBuilder filtered;
        filtered.from = makeSubqueryTableExpression(flagged.build());
        for (const auto & column : current.columns)
            filtered.select.push_back(makeIdentifier(column.name));
        filtered.where = makeIdentifier(flag);
        if (scope.uses_row_number)
        {
            /// Keep the order of the rows.
            flagged.order_by.emplace_back(makeIdentifier("__qrow"), false);
            filtered.from = makeSubqueryTableExpression(flagged.build());
        }
        Names keys = current.keys;
        current = *makeTable(filtered.build(), keys, current.qtypes).table;
    }

    apply_pending();
    return current;
}

QValue QCompiler::compileSQL(const QSQLTemplate & sql, QEnvironment & env, QScope * /*scope*/, size_t offset)
{
    QValue from = compile(sql.from, env, nullptr);
    if (from.kind != QValueKind::Table)
        throwQError(ErrorCodes::TYPE_MISMATCH, offset, "The source of a q-sql template must be a table");

    switch (sql.kind)
    {
        case QSQLTemplate::Kind::Select:
            return compileSelect(sql, *from.table, env);
        case QSQLTemplate::Kind::Exec:
            return compileExec(sql, *from.table, env, offset);
        case QSQLTemplate::Kind::Update:
            return compileUpdate(sql, *from.table, env);
        case QSQLTemplate::Kind::Delete:
            return compileDelete(sql, *from.table, env);
    }
    throwQError(ErrorCodes::SYNTAX_ERROR, offset, "Unknown q-sql template");
}

QValue QCompiler::compileExec(const QSQLTemplate & sql, const QTable & source, QEnvironment & env, size_t offset)
{
    /// `exec` returns a value instead of a table:
    /// - for one phrase without a name, the value of the phrase: the list of a column, or an atom for an aggregate;
    /// - with the by phrase as well, a dictionary from the keys to those values;
    /// - for several phrases or a named phrase, q returns a dictionary from the names of the columns to their values,
    ///   and without a phrase, the last record. The lists of a dictionary are of one type here, and there are no
    ///   records, so these forms are not supported. A table in place of them would give other results without
    ///   a notice: its `count` is the number of the rows, not of the columns.
    if (sql.columns.size() != 1 || !sql.columns.front().name.empty())
        throwQError(ErrorCodes::NOT_IMPLEMENTED, offset,
            "'exec' is supported with one phrase without a name: with several phrases, with a named phrase, or without "
            "a phrase it returns a dictionary of the columns or a record (use 'select')");

    bool single_atom = false;
    QValue table = compileSelect(sql, source, env, &single_atom);

    auto column_as_list = [&](const NameAndTypePair & column)
    {
        QSelectBuilder builder;
        builder.from = table.table->table_expression;
        builder.select.push_back(makeIdentifier(column.name));
        return toScalar(makeTable(builder.build(), {}, table.table->qtypes), offset);
    };

    if (sql.has_by)
    {
        if (table.table->keys.size() != 1 || table.table->columns.size() != 2)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, offset, "'exec' with several keys in the by phrase is not supported (use 'select')");
        QValue result;
        result.kind = QValueKind::Dictionary;
        result.dictionary = std::make_shared<std::pair<QValue, QValue>>(
            column_as_list(table.table->columns.front()), column_as_list(table.table->columns.back()));
        return result;
    }

    const auto & column = table.table->columns.front();
    if (!single_atom)
        return column_as_list(column);

    /// One row of one atom: a scalar subquery.
    QValue result = scalar(make_intrusive<ASTSubquery>(selectAll(*table.table)));
    QValue as_column = columnValue(column, table.table->qtypes);
    result.depth = as_column.depth;
    result.is_string = as_column.is_string;
    result.qtype = as_column.qtype;
    return result;
}

QValue QCompiler::compileSelect(const QSQLTemplate & sql, const QTable & source, QEnvironment & env, bool * single_atom)
{
    QTable filtered = applyWhere(source, sql.where, env);

    /// `select[n; >c]`: the order and the limit apply to the rows that the where phrase leaves, before the by and select
    /// phrases see them. So `select[2] sum x from t` is the sum of the first two rows.
    if (!sql.order_column.empty() || sql.limit)
    {
        QValue rows;
        rows.kind = QValueKind::Table;
        rows.table = std::make_shared<QTable>(filtered);
        if (!sql.order_column.empty())
        {
            QValue column = scalar(makeLiteral(Field(sql.order_column)));
            column.qtype = QType::Symbol;
            column.is_string = true;
            rows = callBuiltin(sql.order_descending ? "xdesc" : "xasc", {column, rows}, env, nullptr, sql.from ? sql.from->offset : 0);
        }
        if (sql.limit)
        {
            QValue limit = compile(sql.limit, env, nullptr);
            rows = callBuiltin("#", {limit, rows}, env, nullptr, sql.limit->offset);
        }
        filtered = *rows.table;
    }

    std::vector<std::pair<ASTPtr, String>> pre;
    QScope scope;
    scope.columns = filtered.columns;
    scope.qtypes = filtered.qtypes;
    scope.pre_aggregation = &pre;

    /// The by phrase.
    Names key_names;
    ASTs key_asts;
    std::vector<std::optional<QType>> key_qtypes;
    for (const auto & phrase : sql.by)
    {
        QValue key = toScalar(compile(phrase.expression, env, &scope), phrase.expression->offset);
        key_qtypes.push_back(key.qtype);
        if (key.kind == QValueKind::Scalar && key.aggregated)
            throwQError(ErrorCodes::TYPE_MISMATCH, phrase.expression->offset, "A phrase of the by phrase cannot be an aggregate");
        if (key.kind != QValueKind::Scalar && key.kind != QValueKind::Column)
            throwQError(ErrorCodes::TYPE_MISMATCH, phrase.expression->offset, "A phrase of the by phrase must be a column");
        key_names.push_back(phrase.name.empty() ? deriveColumnName(phrase.expression) : phrase.name);
        key_asts.push_back(columnForAggregation(key, &scope));
    }

    bool grouped = sql.has_by;
    if (grouped)
    {
        scope.grouped = true;
        scope.partition = key_asts;
    }

    /// The select phrase.
    Names names;
    QValues values;
    for (const auto & phrase : sql.columns)
    {
        names.push_back(phrase.name.empty() ? deriveColumnName(phrase.expression) : phrase.name);
        values.push_back(toScalar(compile(phrase.expression, env, &scope), phrase.expression->offset));
    }

    if (sql.columns.empty())
    {
        /// `select from t` keeps all the columns; `select by k from t` keeps the last row of each group.
        std::unordered_set<String> keys_set(key_names.begin(), key_names.end());
        for (const auto & column : filtered.columns)
        {
            if (grouped && keys_set.contains(column.name))
                continue;
            names.push_back(column.name);
            QValue value = columnValue(column, filtered.qtypes);
            if (grouped)
            {
                value.kind = QValueKind::Scalar;
                value.aggregated = true;
                value.ast = pickFromColumn("anyLast", value.ast);
            }
            values.push_back(std::move(value));
        }
    }

    for (const auto & value : values)
        if (value.kind != QValueKind::Scalar && value.kind != QValueKind::Column)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "A column of a q-sql template must be an atom, a list, or a column");

    Names all_names = key_names;
    all_names.insert(all_names.end(), names.begin(), names.end());
    makeNamesUnique(all_names);
    Names output_keys(all_names.begin(), all_names.begin() + key_names.size());
    Names output_names(all_names.begin() + key_names.size(), all_names.end());

    QSelectBuilder main;
    QSelectBuilder outer;
    Names inner_names;

    bool aggregate_mode = true;
    if (!grouped)
        for (const auto & value : values)
            aggregate_mode &= value.kind == QValueKind::Scalar;

    if (grouped)
    {
        for (size_t i = 0; i < key_asts.size(); ++i)
        {
            String inner = newName();
            main.select.push_back(withAlias(key_asts[i], inner));
            main.group_by.push_back(makeIdentifier(inner));
            outer.select.push_back(withAlias(makeIdentifier(inner), output_keys[i]));
        }
        for (size_t i = 0; i < values.size(); ++i)
        {
            QValue value = values[i];
            if (value.kind == QValueKind::Column)
            {
                value.ast = collectColumn(columnForAggregation(value, &scope));
                value.kind = QValueKind::Scalar;
                value.depth += 1;
            }
            String inner = newName();
            main.select.push_back(withAlias(value.ast, inner));
            outer.select.push_back(withAlias(makeIdentifier(inner), output_names[i]));
        }
        for (const auto & key : output_keys)
            outer.order_by.emplace_back(makeIdentifier(key), false);
    }
    else if (aggregate_mode)
    {
        /// One row of atoms; the lists become the rows of the result, the atoms are repeated.
        bool any_aggregate = false;
        for (const auto & value : values)
            any_aggregate |= value.aggregated;
        for (size_t i = 0; i < values.size(); ++i)
        {
            String inner = newName();
            main.select.push_back(withAlias(values[i].ast, inner));
            outer.select.push_back(withAlias(makeIdentifier(inner), output_names[i]));
            if (values[i].depth > 0)
                outer.array_join.push_back(makeIdentifier(inner));
        }
        if (!any_aggregate && sql.kind == QSQLTemplate::Kind::Select)
        {
            /// Atoms only, like `select 1 from t`: q repeats them for each row of the table.
            aggregate_mode = false;
            outer.array_join.clear();
        }
        if (single_atom)
            *single_atom = aggregate_mode && values.size() == 1 && values.front().depth == 0 && values.front().aggregated;
    }
    else
    {
        for (size_t i = 0; i < values.size(); ++i)
        {
            QValue value = values[i];
            if (value.kind == QValueKind::Scalar && value.aggregated)
                value = broadcast(value, &scope);
            else if (value.kind == QValueKind::Scalar && value.depth > 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "A list cannot be a column of a select with other columns ('length)");
            String inner = newName();
            main.select.push_back(withAlias(value.ast, inner));
            outer.select.push_back(withAlias(makeIdentifier(inner), output_names[i]));
        }
    }

    main.from = buildSource(filtered, scope, pre);
    if (scope.uses_row_number && !grouped && !aggregate_mode)
        main.order_by.emplace_back(makeIdentifier("__qrow"), false);
    outer.from = makeSubqueryTableExpression(main.build());
    outer.distinct = sql.distinct;

    QTypes result_qtypes;
    if (grouped)
        for (size_t i = 0; i < output_keys.size(); ++i)
            if (key_qtypes[i])
                result_qtypes[output_keys[i]] = *key_qtypes[i];
    for (size_t i = 0; i < output_names.size(); ++i)
        if (values[i].qtype)
            result_qtypes[output_names[i]] = *values[i].qtype;

    Names result_keys = grouped ? output_keys : Names{};
    return makeTable(outer.build(), result_keys, result_qtypes);
}

QValue QCompiler::compileUpdate(const QSQLTemplate & sql, const QTable & source, QEnvironment & env)
{
    std::vector<std::pair<ASTPtr, String>> pre;
    QScope scope;
    scope.columns = source.columns;
    scope.qtypes = source.qtypes;
    scope.pre_aggregation = &pre;

    /// The where phrase selects the rows to update. Aggregates in a constraint apply to the rows selected by the previous constraints.
    ASTPtr condition;
    for (const auto & constraint : sql.where)
    {
        QScope constraint_scope = scope;
        if (condition)
            constraint_scope.partition = {condition};
        QValue value = toScalar(compile(constraint, env, &constraint_scope), constraint->offset);
        scope.uses_row_number |= constraint_scope.uses_row_number;
        if (value.kind == QValueKind::Scalar && value.aggregated)
            value = broadcast(value, &constraint_scope);
        if (value.depth > 0 || (value.kind != QValueKind::Column && value.kind != QValueKind::Scalar))
            throwQError(ErrorCodes::TYPE_MISMATCH, constraint->offset, "A constraint of the where phrase must be a boolean column");
        ASTPtr ast = value.ast;
        if (value.windowed)
            ast = columnForAggregation(value, &scope);
        condition = condition ? ASTPtr(makeASTFunction("and", condition, ast)) : ast;
    }

    for (const auto & phrase : sql.by)
    {
        QValue key = compile(phrase.expression, env, &scope);
        if (key.kind != QValueKind::Column && key.kind != QValueKind::Scalar)
            throwQError(ErrorCodes::TYPE_MISMATCH, phrase.expression->offset, "A phrase of the by phrase must be a column");
        scope.partition.push_back(key.ast);
    }
    if (condition)
        scope.partition.push_back(condition);

    std::unordered_set<String> existing;
    for (const auto & column : source.columns)
        existing.insert(column.name);

    QSelectBuilder main;
    main.select.push_back(make_intrusive<ASTAsterisk>());
    std::map<String, String> replaced;
    Names added;
    QTypes result_qtypes = source.qtypes;
    for (const auto & phrase : sql.columns)
    {
        String name = phrase.name.empty() ? deriveColumnName(phrase.expression) : phrase.name;
        QValue value = toScalar(compile(phrase.expression, env, &scope), phrase.expression->offset);
        if (value.qtype)
            result_qtypes[name] = *value.qtype;
        else
            result_qtypes.erase(name);
        if (value.kind == QValueKind::Scalar && value.aggregated)
            value = broadcast(value, &scope);
        if (value.kind == QValueKind::Scalar && value.depth > 0)
            throwQError(ErrorCodes::NOT_IMPLEMENTED, phrase.expression->offset, "A list cannot be a column of update ('length)");
        if (value.kind != QValueKind::Scalar && value.kind != QValueKind::Column)
            throwQError(ErrorCodes::TYPE_MISMATCH, phrase.expression->offset, "A column of update must be an atom or a column");

        ASTPtr ast = value.ast;
        if (condition)
            ast = makeASTFunction("if", condition, ast, existing.contains(name) ? makeIdentifier(name) : makeLiteral(Field()));

        String inner = newName();
        main.select.push_back(withAlias(ast, inner));
        if (existing.contains(name))
            replaced[name] = inner;
        else if (std::find(added.begin(), added.end(), name) == added.end())
        {
            added.push_back(name);
            replaced[name] = inner;
        }
        else
            replaced[name] = inner;
    }

    main.from = buildSource(source, scope, pre);
    if (scope.uses_row_number)
        main.order_by.emplace_back(makeIdentifier("__qrow"), false);

    QSelectBuilder outer;
    outer.from = makeSubqueryTableExpression(main.build());
    for (const auto & column : source.columns)
    {
        if (auto it = replaced.find(column.name); it != replaced.end())
            outer.select.push_back(withAlias(makeIdentifier(it->second), column.name));
        else
            outer.select.push_back(makeIdentifier(column.name));
    }
    for (const auto & name : added)
        outer.select.push_back(withAlias(makeIdentifier(replaced[name]), name));

    return makeTable(outer.build(), source.keys, result_qtypes);
}

QValue QCompiler::compileDelete(const QSQLTemplate & sql, const QTable & source, QEnvironment & env)
{
    if (!sql.columns.empty())
    {
        std::unordered_set<String> deleted;
        for (const auto & phrase : sql.columns)
        {
            if (phrase.expression->kind != QNodeKind::Name)
                throwQError(ErrorCodes::SYNTAX_ERROR, phrase.expression->offset, "'delete' takes the names of the columns to delete");
            deleted.insert(phrase.expression->name);
        }
        QSelectBuilder builder;
        builder.from = source.table_expression;
        Names keys;
        for (const auto & column : source.columns)
        {
            if (deleted.contains(column.name))
                continue;
            builder.select.push_back(makeIdentifier(column.name));
        }
        for (const auto & key : source.keys)
            if (!deleted.contains(key))
                keys.push_back(key);
        if (builder.select.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'delete' cannot delete all the columns of a table");
        return makeTable(builder.build(), keys, source.qtypes);
    }

    std::vector<std::pair<ASTPtr, String>> pre;
    QScope scope;
    scope.columns = source.columns;
    scope.qtypes = source.qtypes;
    scope.pre_aggregation = &pre;

    ASTPtr condition;
    for (const auto & constraint : sql.where)
    {
        QScope constraint_scope = scope;
        if (condition)
            constraint_scope.partition = {condition};
        QValue value = toScalar(compile(constraint, env, &constraint_scope), constraint->offset);
        scope.uses_row_number |= constraint_scope.uses_row_number;
        if (value.kind == QValueKind::Scalar && value.aggregated)
            value = broadcast(value, &constraint_scope);
        if (value.depth > 0 || (value.kind != QValueKind::Column && value.kind != QValueKind::Scalar))
            throwQError(ErrorCodes::TYPE_MISMATCH, constraint->offset, "A constraint of the where phrase must be a boolean column");
        condition = condition ? ASTPtr(makeASTFunction("and", condition, value.ast)) : value.ast;
    }
    if (!condition)
    {
        /// `delete from t` without the where phrase deletes all the rows:
        /// the result has the columns and the keys of the table, and no rows.
        QSelectBuilder empty;
        empty.from = source.table_expression;
        for (const auto & column : source.columns)
            empty.select.push_back(makeIdentifier(column.name));
        empty.limit = makeLiteral(Field(static_cast<UInt64>(0)));
        return makeTable(empty.build(), source.keys, source.qtypes);
    }

    String flag = newName();
    QSelectBuilder main;
    main.from = buildSource(source, scope, pre);
    main.select.push_back(make_intrusive<ASTAsterisk>());
    main.select.push_back(withAlias(makeASTFunction("ifNull", condition, makeLiteral(Field(false))), flag));
    if (scope.uses_row_number)
        main.order_by.emplace_back(makeIdentifier("__qrow"), false);

    QSelectBuilder outer;
    outer.from = makeSubqueryTableExpression(main.build());
    for (const auto & column : source.columns)
        outer.select.push_back(makeIdentifier(column.name));
    outer.where = makeASTFunction("not", makeIdentifier(flag));
    return makeTable(outer.build(), source.keys, source.qtypes);
}

ASTPtr QCompiler::finalQuery(const QValue & value, const QNodePtr & statement)
{
    switch (value.kind)
    {
        case QValueKind::Table:
            return selectAll(*value.table);
        case QValueKind::Scalar:
        {
            QSelectBuilder builder;
            builder.select.push_back(withAlias(value.ast, deriveColumnName(statement)));
            return builder.build();
        }
        case QValueKind::Dictionary:
        {
            const auto & [keys, values] = *value.dictionary;
            QSelectBuilder inner;
            inner.select.push_back(withAlias(keys.ast, "key"));
            inner.select.push_back(withAlias(values.ast, "value"));
            QSelectBuilder outer;
            outer.from = makeSubqueryTableExpression(inner.build());
            outer.select = {makeIdentifier("key"), makeIdentifier("value")};
            outer.array_join = {makeIdentifier("key"), makeIdentifier("value")};
            return outer.build();
        }
        case QValueKind::Function:
        {
            QSelectBuilder builder;
            String source = value.function->lambda ? value.function->lambda->source : value.function->builtin + value.function->adverb;
            builder.select.push_back(withAlias(makeLiteral(Field(source)), "x"));
            return builder.build();
        }
        case QValueKind::Column:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "A column cannot be the result of a q program");
}

ASTPtr QCompiler::compileProgram(const QProgram & program)
{
    CopyBudget previous_budget = copy_budget;
    copy_budget = CopyBudget{max_expanded_elements, 0};
    SCOPE_EXIT({ copy_budget = previous_budget; });

    QEnvironment globals;
    std::optional<QValue> result;
    QNodePtr result_statement;
    for (const auto & statement : program.statements)
    {
        if (!statement)
            continue;
        result = compile(statement, globals, nullptr);
        result_statement = statement;
    }
    if (!result)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty q program");
    return finalQuery(*result, result_statement);
}

}

ASTPtr compileQProgram(const ASTQQuery & query, ContextPtr context)
{
    QCompilerInternals::QCompiler compiler(std::move(context));
    return compiler.compileProgram(*query.program);
}

}
