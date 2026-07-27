#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnSet.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/PreparedSets.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ExpressionOperatorPrettyLookup.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <optional>
#include <string_view>

namespace DB
{

namespace QueryPlanFormat
{
    constexpr std::string_view TABLE_PREFIX = "__table";

    /// Matches `__table<digits>.` at position pos, returns the position after the dot or 0 on mismatch.
    size_t matchTablePrefix(std::string_view name, size_t pos)
    {
        if (!name.substr(pos).starts_with(TABLE_PREFIX))
            return 0;
        size_t j = pos + TABLE_PREFIX.size();
        while (j < name.size() && std::isdigit(static_cast<unsigned char>(name[j])))
            ++j;
        if (j > pos + TABLE_PREFIX.size() && j < name.size() && name[j] == '.')
            return j + 1;
        return 0;
    }

    String trimColumnIdentifier(std::string_view name)
    {
        if (name.find(TABLE_PREFIX) == std::string_view::npos)
            return String(name);

        String result;
        result.reserve(name.size());
        size_t seg_start = 0;
        for (size_t i = 0; i < name.size();)
        {
            if (size_t after = matchTablePrefix(name, i))
            {
                result.append(name, seg_start, i - seg_start);
                i = after;
                seg_start = after;
            }
            else
            {
                ++i;
            }
        }
        result.append(name, seg_start, name.size() - seg_start);
        return result;
    }

    void formatJoinOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix)
    {
        const auto & input_headers = step.getInputHeaders();
        if (input_headers.size() != 2 || !input_headers[0] || !input_headers[1])
            return;

        out << prefix << "Output:\n";

        if (!step.hasOutputHeader() || step.getOutputHeader()->empty())
        {
            out << prefix << "  Left:  Empty\n";
            out << prefix << "  Right: Empty\n";
            return;
        }

        const auto & output = *step.getOutputHeader();
        const auto & left_input = *input_headers[0];
        const auto & right_input = *input_headers[1];

        std::vector<String> left_columns;
        std::vector<String> right_columns;

        for (const auto & col : output)
        {
            if (left_input.has(col.name))
                left_columns.push_back(trimColumnIdentifier(col.name));
            else if (right_input.has(col.name))
                right_columns.push_back(trimColumnIdentifier(col.name));
        }

        out << prefix << "  Left:  ";
        if (left_columns.empty())
            out << "Empty";
        else
            out << fmt::format("{}", fmt::join(left_columns, ", "));
        out << "\n";

        out << prefix << "  Right: ";
        if (right_columns.empty())
            out << "Empty";
        else
            out << fmt::format("{}", fmt::join(right_columns, ", "));
        out << "\n";
    }

    void formatOutputColumns(const std::unordered_map<String, PrettyColumnName> & pretty_names, WriteBuffer & out, const IQueryPlanStep & step, const String & prefix)
    {
        if (!step.hasOutputHeader() || step.getOutputHeader()->empty())
        {
            out << prefix << "Output: Empty\n";
            return;
        }

        out << prefix << "Output: ";
        bool first = true;
        for (const auto & elem : *step.getOutputHeader())
        {
            if (!first)
                out << ", ";
            first = false;
            auto it = pretty_names.find(elem.name);
            String pretty_name = it != pretty_names.end() ? it->second.expression : trimColumnIdentifier(elem.name);

            out << pretty_name;
        }
        out << '\n';
    }

    PrettyColumnName formatFilterPretty(
        const ActionsDAG & dag,
        const String & column_name,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
    {
        const auto * root = dag.tryFindInOutputs(column_name);
        if (!root)
            return PrettyColumnName(trimColumnIdentifier(column_name));

        auto atoms = ActionsDAG::extractConjunctionAtoms(root);

        std::vector<String> user_parts;
        std::vector<String> rf_parts;
        for (const auto * atom : atoms)
        {
            if (atom->type == ActionsDAG::ActionType::FUNCTION
                && atom->function_base
                && atom->function_base->getName() == "__applyFilter")
                rf_parts.push_back(formatNodePretty(atom, pretty_names, runtime_filter_names, subquery_set_names, 4));
            else
                user_parts.push_back(formatNodePretty(atom, pretty_names, runtime_filter_names, subquery_set_names, 4));
        }

        String expression;
        if (!user_parts.empty())
            expression = fmt::format(" {}", fmt::join(user_parts, " AND "));
        if (expression.empty() && rf_parts.empty())
            expression = fmt::format(" {}", trimColumnIdentifier(column_name));

        String annotation;
        if (!rf_parts.empty())
            annotation = fmt::format("Runtime filters: {}", fmt::join(rf_parts, " AND "));

        return {std::move(expression), std::move(annotation)};
    }

    namespace
    {
        struct OperatorInfo
        {
            std::string_view symbol;
            int precedence;
        };

        std::optional<OperatorInfo> getOperatorInfo(const std::string & func_name)
        {
            if (auto info = tryGetExpressionOperatorPrettyInfo(func_name))
                return OperatorInfo{info->symbol, info->precedence};
            return std::nullopt;
        }

        String formatConstant(const ActionsDAG::Node * node)
        {
            if (!node->column || node->column->empty())
                return node->result_name;

            if (node->result_type && WhichDataType(node->result_type).isSet())
                return node->result_name;

            WhichDataType data_type(node->result_type);

            if (data_type.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64())
            {
                WriteBufferFromOwnString buf;
                writeChar('\'', buf);
                const auto & col = node->column->convertToFullColumnIfConst();
                node->result_type->getDefaultSerialization()->serializeText(*col, 0, buf, {});
                writeChar('\'', buf);
                return buf.str();
            }

            Field value;
            node->column->get(0, value);
            return applyVisitor(FieldVisitorToString(), value);
        }

        String getRuntimeFilterId(const ActionsDAG::Node * node)
        {
            const ActionsDAG::Node * first_child = node->children[0];
            if (const auto * col = checkAndGetColumnConst<ColumnString>(first_child->column.get()))
                return col->getValue<String>();
            return first_child->result_name;
        }

        String formatSetPretty(
            const ActionsDAG::Node * set_node,
            std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
        {
            static constexpr size_t MAX_SET_ELEMENTS_TO_SHOW = 10;

            if (!set_node->column)
                return trimColumnIdentifier(set_node->result_name);

            const auto * column_ptr = set_node->column.get();
            const auto * col_const = typeid_cast<const ColumnConst *>(column_ptr);
            const ColumnSet * column_set = col_const ? typeid_cast<const ColumnSet *>(&col_const->getDataColumn()) : typeid_cast<const ColumnSet *>(column_ptr);

            if (!column_set || !column_set->getData())
                return trimColumnIdentifier(set_node->result_name);

            FutureSetPtr future_set = column_set->getData();

            if (const auto * from_storage = typeid_cast<const FutureSetFromStorage *>(future_set.get()))
            {
                if (auto storage_id = from_storage->getStorageID())
                    return storage_id->getFullNameNotQuoted();
                return trimColumnIdentifier(set_node->result_name);
            }

            if (typeid_cast<const FutureSetFromSubquery *>(future_set.get()))
            {
                auto [it, inserted] = subquery_set_names.try_emplace(
                    future_set->getHash(),
                    fmt::format("subquery{}", subquery_set_names.size() + 1));
                return it->second;
            }

            if (auto * from_tuple = typeid_cast<FutureSetFromTuple *>(future_set.get()))
            {
                Columns key_columns = from_tuple->getKeyColumns();
                if (key_columns.empty())
                    return trimColumnIdentifier(set_node->result_name);

                size_t num_rows = key_columns[0]->size();
                size_t num_keys = key_columns.size();
                size_t to_show = std::min(num_rows, MAX_SET_ELEMENTS_TO_SHOW);

                String result = "(";
                for (size_t row = 0; row < to_show; ++row)
                {
                    if (row > 0)
                        result += ", ";
                    if (num_keys > 1)
                        result += "(";
                    for (size_t col = 0; col < num_keys; ++col)
                    {
                        if (col > 0)
                            result += ", ";
                        Field value;
                        key_columns[col]->get(row, value);
                        result += applyVisitor(FieldVisitorToString(), value);
                    }
                    if (num_keys > 1)
                        result += ")";
                }
                if (num_rows > to_show)
                    result += fmt::format(", ... {} more", num_rows - to_show);
                result += ")";
                return result;
            }

            return trimColumnIdentifier(set_node->result_name);
        }
    }

    String formatNodePretty(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names,
        int parent_precedence)
    {
        using ActionType = ActionsDAG::ActionType;

        switch (node->type)
        {
            case ActionType::INPUT:
            {
                if (auto it = pretty_names.find(node->result_name); it != pretty_names.end())
                    return it->second.expression;
                return trimColumnIdentifier(node->result_name);
            }

            case ActionType::COLUMN:
                return formatConstant(node);
            case ActionType::ALIAS:
                return formatNodePretty(node->children.front(), pretty_names, runtime_filter_names, subquery_set_names, parent_precedence);

            case ActionType::ARRAY_JOIN:
                return "arrayJoin(" + formatNodePretty(node->children.front(), pretty_names, runtime_filter_names, subquery_set_names) + ")";

            case ActionType::FUNCTION:
            {
                auto func_name = node->function_base->getName();

                if (func_name == "__applyFilter")
                {
                    String filter_id = getRuntimeFilterId(node);
                    const auto * probe_node = node->children[1];
                    String probe_column;
                    if (auto pit = pretty_names.find(probe_node->result_name); pit != pretty_names.end())
                        probe_column = pit->second.expression;
                    else
                        probe_column = trimColumnIdentifier(probe_node->result_name);

                    if (auto it = runtime_filter_names.find(filter_id); it != runtime_filter_names.end())
                    {
                        const auto & pretty_filter_name = it->second.pretty_name;
                        const auto & build_column = it->second.build_column_name;
                        const auto & build_table = it->second.build_table_name;
                        if (build_table.empty())
                            return fmt::format("{}({}, {})", pretty_filter_name, probe_column, build_column);
                        return fmt::format("{}({}, {} from {})", pretty_filter_name, probe_column, build_column, build_table);
                    }
                    return fmt::format("{}({})", filter_id, probe_column);
                }

                if ((func_name == "_CAST" || func_name == "CAST") && node->children.size() == 2)
                {
                    auto inner = formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names);
                    Field type_field;
                    node->children[1]->column->get(0, type_field);
                    return "CAST(" + inner + " AS " + type_field.safeGet<String>() + ")";
                }

                auto op_info = getOperatorInfo(func_name);

                if (func_name == "not" && node->children.size() == 1)
                {
                    String result = "NOT " + formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "negate" && node->children.size() == 1)
                {
                    String result = "-" + formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "isNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence) + " IS NULL";

                if (func_name == "isNotNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence) + " IS NOT NULL";

                if ((func_name == "and" || func_name == "or") && node->children.size() >= 2)
                {
                    String separator = fmt::format(" {} ", op_info->symbol);
                    std::vector<String> parts;
                    parts.reserve(node->children.size());
                    for (const auto * child : node->children)
                        parts.push_back(formatNodePretty(child, pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence));

                    String result = fmt::format("{}", fmt::join(parts, separator));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "arrayElement" && node->children.size() == 2)
                {
                    auto arr = formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence);
                    auto idx = formatNodePretty(node->children[1], pretty_names, runtime_filter_names, subquery_set_names);
                    return arr + "[" + idx + "]";
                }

                if (func_name == "tupleElement" && node->children.size() == 2)
                {
                    auto tup = formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence);
                    auto elem = formatNodePretty(node->children[1], pretty_names, runtime_filter_names, subquery_set_names);
                    return tup + "." + elem;
                }

                if (op_info && (op_info->symbol == "IN" || op_info->symbol == "NOT IN")
                    && node->children.size() == 2)
                {
                    auto lhs = formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence);
                    auto rhs = formatSetPretty(node->children[1], subquery_set_names);
                    String result = fmt::format("{} {} {}", lhs, op_info->symbol, rhs);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (op_info && !op_info->symbol.empty() && node->children.size() == 2)
                {
                    String result = fmt::format("{} {} {}",
                        formatNodePretty(node->children[0], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence),
                        op_info->symbol,
                        formatNodePretty(node->children[1], pretty_names, runtime_filter_names, subquery_set_names, op_info->precedence));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                std::vector<String> args;
                args.reserve(node->children.size());
                for (const auto * child : node->children)
                    args.push_back(formatNodePretty(child, pretty_names, runtime_filter_names, subquery_set_names));

                return func_name + "(" + fmt::format("{}", fmt::join(args, ", ")) + ")";
            }

            default:
                return node->result_name;
        }
    }

    String formatColumnPretty(const String & column_name, const std::unordered_map<String, PrettyColumnName> & pretty_names)
    {
        if (auto it = pretty_names.find(column_name); it != pretty_names.end())
            return it->second.expression;
        return trimColumnIdentifier(column_name);
    }

    std::string_view getColumnAnnotation(const String & column_name, const ExplainFormatSettings & settings)
    {
        if (auto it = settings.pretty_names.find(column_name); it != settings.pretty_names.end())
            return it->second.annotation;
        return {};
    }

    static void addAggregatesPrettyNames(const Aggregator::Params & params, std::unordered_map<String, PrettyColumnName> & pretty_names)
    {
        for (const auto & agg : params.aggregates)
        {
            String pretty;
            if (agg.function)
                pretty += agg.function->getName();

            const Array & aggregate_parameters = agg.function ? agg.function->getParameters() : agg.parameters;
            bool first_param = true;
            for (const auto & param : aggregate_parameters)
            {
                pretty += first_param ? "(" : ", ";
                first_param = false;
                pretty += applyVisitor(FieldVisitorToString(), param);
            }
            if (!aggregate_parameters.empty())
                pretty += ')';

            pretty += '(';
            bool first = true;
            for (const auto & arg : agg.argument_names)
            {
                if (!first)
                    pretty += ", ";
                first = false;
                if (auto it = pretty_names.find(arg); it != pretty_names.end())
                    pretty += it->second.expression;
                else
                    pretty += trimColumnIdentifier(arg);
            }
            pretty += ')';
            pretty_names.try_emplace(agg.column_name, PrettyColumnName(std::move(pretty)));
        }
    }

    static void addWindowFunctionPrettyNames(const WindowDescription & window_description, std::unordered_map<String, PrettyColumnName> & pretty_names)
    {
        String spec = "(";

        if (!window_description.partition_by.empty())
        {
            spec += "PARTITION BY ";
            for (size_t i = 0; i < window_description.partition_by.size(); ++i)
            {
                if (i > 0)
                    spec += ", ";
                spec += formatColumnPretty(window_description.partition_by[i].column_name, pretty_names);
            }
        }

        if (!window_description.partition_by.empty() && !window_description.order_by.empty())
            spec += ' ';

        if (!window_description.order_by.empty())
        {
            spec += "ORDER BY ";
            for (size_t i = 0; i < window_description.order_by.size(); ++i)
            {
                if (i > 0)
                    spec += ", ";
                const auto & desc = window_description.order_by[i];
                spec += formatColumnPretty(desc.column_name, pretty_names);
                spec += desc.direction > 0 ? " ASC" : " DESC";
                if (desc.with_fill)
                    spec += " WITH FILL";
            }
        }

        if (!window_description.frame.is_default)
        {
            if (!window_description.partition_by.empty() || !window_description.order_by.empty())
                spec += ' ';
            spec += window_description.frame.toString();
        }

        spec += ')';

        for (const auto & func : window_description.window_functions)
        {
            String pretty;

            if (func.aggregate_function)
                pretty += func.aggregate_function->getName();

            pretty += '(';
            for (size_t i = 0; i < func.argument_names.size(); ++i)
            {
                if (i > 0)
                    pretty += ", ";
                pretty += formatColumnPretty(func.argument_names[i], pretty_names);
            }
            pretty += ") OVER ";
            pretty += spec;

            pretty_names[func.column_name] = PrettyColumnName(std::move(pretty));
        }
    }

    static String findSourceTableName(const QueryPlan::Node * node)
    {
        while (node)
        {
            const auto & step_name = node->step->getName();
            if (step_name == "ReadFromMergeTree")
                return static_cast<const ReadFromMergeTree *>(node->step.get())->getStorageID().getFullNameNotQuoted();
            if (step_name == "ReadFromRemoteParallelReplicas")
                return static_cast<const ReadFromParallelRemoteReplicasStep *>(node->step.get())->getStorageID().getFullNameNotQuoted();
            if (step_name == "ReadFromStorage")
                return static_cast<const ReadFromStorageStep *>(node->step.get())->getStorage()->getStorageID().getFullNameNotQuoted();
            if (step_name == "ReadFromMemoryStorage")
                return static_cast<const ReadFromMemoryStorageStep *>(node->step.get())->getStorage()->getStorageID().getFullNameNotQuoted();
            if (step_name == "ReadFromTable")
                return static_cast<const ReadFromTableStep *>(node->step.get())->getTable();
            if (node->children.size() == 1)
                node = node->children[0];
            else
                break;
        }
        return {};
    }

    static void buildPrettyNamesForNode(
        const QueryPlan::Node * node,
        std::unordered_map<String, PrettyColumnName> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
    {
        for (auto it = node->children.rbegin(); it != node->children.rend(); ++it)
            buildPrettyNamesForNode(*it, pretty_names, runtime_filter_names, subquery_set_names);

        const auto & step = node->step;
        const auto & step_name = step->getName();

        if (step_name == "Expression")
        {
            const auto & dag = static_cast<const ExpressionStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, pretty_names, runtime_filter_names, subquery_set_names));
        }
        else if (step_name == "Filter")
        {
            const auto & dag = static_cast<const FilterStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, pretty_names, runtime_filter_names, subquery_set_names));
        }
        else if (step_name == "Aggregating" || step_name == "AggregatingProjection")
        {
            addAggregatesPrettyNames(static_cast<const AggregatingStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "MergingAggregated")
        {
            addAggregatesPrettyNames(static_cast<const MergingAggregatedStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "TotalsHaving")
        {
            const auto * having_step = static_cast<const TotalsHavingStep *>(step.get());
            if (const auto * dag = having_step->getActions())
            {
                for (const auto * output : dag->getOutputs())
                    if (output->type != ActionsDAG::ActionType::INPUT)
                        pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, pretty_names, runtime_filter_names, subquery_set_names));
            }
        }
        else if (step_name == "Window")
        {
            const auto * window_step = static_cast<const WindowStep *>(step.get());
            addWindowFunctionPrettyNames(window_step->getWindowDescription(), pretty_names);
        }
        else if (step_name == "BuildRuntimeFilter")
        {
            const auto * rf_step = static_cast<const BuildRuntimeFilterStep *>(step.get());
            String pretty_name = fmt::format("RF{}", runtime_filter_names.size() + 1);

            const auto & filter_col = rf_step->getFilterColumnName();
            String build_column;
            if (auto it = pretty_names.find(filter_col); it != pretty_names.end())
                build_column = it->second.expression;
            else
                build_column = trimColumnIdentifier(filter_col);

            String build_table = findSourceTableName(node);
            runtime_filter_names.try_emplace(rf_step->getFilterName(),
                RuntimeFilterInfo{std::move(pretty_name), std::move(build_column), std::move(build_table)});
        }

        if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(step.get()))
        {
            if (auto prewhere = source->getPrewhereInfo())
            {
                pretty_names[prewhere->prewhere_column_name] = formatFilterPretty(
                    prewhere->prewhere_actions,
                    prewhere->prewhere_column_name,
                    pretty_names,
                    runtime_filter_names,
                    subquery_set_names);
            }
            if (auto row_level = source->getRowLevelFilter())
            {
                pretty_names[row_level->column_name] = formatFilterPretty(
                    row_level->actions,
                    row_level->column_name,
                    pretty_names,
                    runtime_filter_names,
                    subquery_set_names);
            }

            if (step_name == "ReadFromMergeTree")
            {
                const auto * read_from_merge_tree_step = static_cast<const ReadFromMergeTree *>(step.get());
                if (auto deferred_row_level_filter = read_from_merge_tree_step->getDeferredRowLevelFilter())
                {
                    pretty_names[deferred_row_level_filter->column_name] = formatFilterPretty(
                        deferred_row_level_filter->actions,
                        deferred_row_level_filter->column_name,
                        pretty_names,
                        runtime_filter_names,
                        subquery_set_names);
                }
                if (auto deferred_prewhere = read_from_merge_tree_step->getDeferredPrewhereInfo())
                {
                    pretty_names[deferred_prewhere->prewhere_column_name] = formatFilterPretty(
                        deferred_prewhere->prewhere_actions,
                        deferred_prewhere->prewhere_column_name,
                        pretty_names,
                        runtime_filter_names,
                        subquery_set_names);
                }
            }
        }
    }

    void buildPrettyNamesMap(
        const QueryPlan & plan,
        std::unordered_map<String, PrettyColumnName> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
    {
        if (plan.getRootNode())
            buildPrettyNamesForNode(plan.getRootNode(), pretty_names, runtime_filter_names, subquery_set_names);
    }

}

}
