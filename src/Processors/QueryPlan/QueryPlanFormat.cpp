#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
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
#include <stack>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace QueryPlanFormat
{
    constexpr std::string_view TABLE_PREFIX = "__table";

    /// Matches `__table<digits>.` at position pos, returns the position after the dot or 0 on mismatch.
    static size_t matchTablePrefix(std::string_view name, size_t pos)
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
        if (!name.contains(TABLE_PREFIX))
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
            /// A masked secret constant must render as `[HIDDEN]`, never as the value held in its
            /// column (kept only so the query can still execute). `is_masked_secret` is the reliable
            /// signal; the name check is a fallback for a masked constant whose name is its `[HIDDEN...]`
            /// placeholder but which was reached without the flag (e.g. an aliased column keeps its own
            /// name, so the flag is what catches it there).
            if (node->is_masked_secret)
                return "[HIDDEN]";
            if (node->result_name.contains("[HIDDEN"))
                return node->result_name;

            if (!node->column)
                return node->result_name;

            if (node->result_type && WhichDataType(node->result_type).isSet())
                return node->result_name;

            /// node->column is a size-0 ColumnConst; read the value from its data column.
            const auto & data_col = node->column->getDataColumnPtr();
            WhichDataType data_type(node->result_type);

            if (data_type.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64())
            {
                WriteBufferFromOwnString buf;
                writeChar('\'', buf);
                node->result_type->getDefaultSerialization()->serializeText(*data_col, 0, buf, {});
                writeChar('\'', buf);
                return buf.str();
            }

            Field value;
            data_col->get(0, value);
            return applyVisitor(FieldVisitorToString(), value);
        }

        String getRuntimeFilterId(const ActionsDAG::Node * node)
        {
            /// The first `__applyFilter` argument is a const whose DAG result NAME is the stable
            /// structural id (`_runtime_filter_<hash>`), under which `runtime_filter_names` is keyed
            /// (`BuildRuntimeFilterStep::getFilterName`). Its VALUE is the volatile per-plan-build
            /// rendezvous key, which must never surface in EXPLAIN — so key on the result name.
            return node->children[0]->result_name;
        }

        /// An ActionsDAG node reached by several parents is one node in memory, but rendering the DAG as
        /// a tree prints its whole subtree once per path that reaches it, so the text is exponential in
        /// expression depth while the DAG stays small. Hence a cap on one rendered expression.
        constexpr size_t MAX_EXPRESSION_LENGTH = 8192;
        constexpr std::string_view TRUNCATED_MARKER = "...";

        /// One budget is shared by every recursive call rendering a single expression. A per-call
        /// allowance would leave the total exponential, since each sibling would start over.
        struct LengthBudget
        {
            size_t remaining = MAX_EXPRESSION_LENGTH;

            bool exhausted() const { return remaining == 0; }
            void charge(size_t size) { remaining -= std::min(size, remaining); }

            /// Returned text is clipped to what is left, so a single oversized leaf (an already-rendered
            /// column name substituted from `pretty_names`) cannot carry the expression past the cap.
            String take(String text)
            {
                if (text.size() > remaining)
                {
                    text.resize(remaining);
                    text += TRUNCATED_MARKER;
                    remaining = 0;
                    return text;
                }
                remaining -= text.size();
                return text;
            }
        };

        /// Counted per class rather than per conjunction, because each class below is rendered as its own
        /// output line under its own budget. Larger than the atom count either budget can render, since
        /// a rendered atom costs a character and all but the first also a five-character separator, so
        /// the cap can only drop atoms that would not have been printed.
        constexpr size_t MAX_CONJUNCTION_ATOMS_PER_CLASS = MAX_EXPRESSION_LENGTH / 2;

        bool isRuntimeFilterAtom(const ActionsDAG::Node * node)
        {
            return node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
                && node->function_base->getName() == "__applyFilter";
        }

        struct ConjunctionAtoms
        {
            ActionsDAG::NodeRawConstPtrs user_atoms;
            ActionsDAG::NodeRawConstPtrs runtime_filter_atoms;
            bool user_truncated = false;
            bool runtime_filter_truncated = false;
            /// A user atom was left unreached, so an empty user class means "not reached" rather than
            /// "not present". The atom lists alone cannot tell those apart.
            bool user_atom_unreached = false;
        };

        /// Splits a conjunction like `ActionsDAG::extractConjunctionAtoms`, but stops at `max_atoms` per
        /// class: that walk yields one atom per path into a shared subtree, so it grows with conjunction
        /// depth while the DAG does not. No visited set, so shown atoms stay the ones the optimizer sees.
        ConjunctionAtoms extractConjunctionAtomsBounded(const ActionsDAG::Node * predicate, size_t max_atoms)
        {
            /// The quotas bound what is stored; a conjunction of them cannot bound the walk, because a
            /// query holding only one class never fills the other. Both classes together store at most
            /// twice `max_atoms`, which already exceeds the atoms a budget can render.
            const size_t max_visits = 2 * max_atoms;
            size_t visits = 0;

            ConjunctionAtoms result;

            std::stack<const ActionsDAG::Node *> stack;
            stack.push(predicate);

            while (!stack.empty())
            {
                if (result.user_atoms.size() >= max_atoms && result.runtime_filter_atoms.size() >= max_atoms)
                    break;

                if (visits >= max_visits)
                    break;

                ++visits;
                const auto * node = stack.top();
                stack.pop();
                if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
                    && node->function_base->getName() == "and")
                {
                    for (const auto * arg : node->children)
                        stack.push(arg);

                    continue;
                }

                /// Classified once here, so a dropped atom costs no second function-name lookup.
                const bool is_runtime_filter = isRuntimeFilterAtom(node);
                auto & atoms = is_runtime_filter ? result.runtime_filter_atoms : result.user_atoms;
                bool & truncated = is_runtime_filter ? result.runtime_filter_truncated : result.user_truncated;

                if (atoms.size() >= max_atoms)
                    truncated = true;
                else
                    atoms.push_back(node);
            }

            /// What is outstanding decides which line is short of content and whether an empty class is
            /// absent or merely unreached; either class can be the one left, so the stop alone cannot say.
            /// Presence is all that is asked, so one visit per node answers it and needs no bound.
            std::unordered_set<const ActionsDAG::Node *> seen;
            while (!stack.empty())
            {
                const auto * node = stack.top();
                stack.pop();
                if (!seen.insert(node).second)
                    continue;

                if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
                    && node->function_base->getName() == "and")
                {
                    for (const auto * arg : node->children)
                        stack.push(arg);

                    continue;
                }

                if (isRuntimeFilterAtom(node))
                    result.runtime_filter_truncated = true;
                else
                    result.user_atom_unreached = true;
            }

            /// A class is short of content only if something of that class was actually left behind, so a
            /// class the scan proves complete keeps no marker even though the walk itself stopped early.
            if (result.user_atom_unreached && !result.user_atoms.empty())
                result.user_truncated = true;

            return result;
        }

        String formatSetPretty(
            const ActionsDAG::Node * set_node,
            std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
        {
            static constexpr size_t MAX_SET_ELEMENTS_TO_SHOW = 10;

            if (!set_node->column)
                return trimColumnIdentifier(set_node->result_name);

            const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&set_node->column->getDataColumn());

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

    static String formatNodePrettyImpl(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names,
        int parent_precedence,
        LengthBudget & budget)
    {
        using ActionType = ActionsDAG::ActionType;

        /// Stopping the descent once the shared budget is spent is what bounds the expression, however
        /// many parents reach a shared subtree.
        if (budget.exhausted())
            return String(TRUNCATED_MARKER);

        auto charge = [&budget](size_t size) { budget.charge(size); };
        auto emit = [&budget](String text) -> String { return budget.take(std::move(text)); };
        auto recurse = [&](const ActionsDAG::Node * child, int precedence)
        {
            return formatNodePrettyImpl(child, pretty_names, runtime_filter_names, subquery_set_names, precedence, budget);
        };

        /// A masked secret carrier (a folded constant, which may be a FUNCTION node with a constant
        /// column, not only a COLUMN node) must render as `[HIDDEN]` regardless of its node type,
        /// before we dispatch into formatting its value or its child expression.
        if (node->is_masked_secret)
            return emit("[HIDDEN]");

        switch (node->type)
        {
            case ActionType::INPUT:
            {
                if (auto it = pretty_names.find(node->result_name); it != pretty_names.end())
                    return emit(it->second.expression);
                return emit(trimColumnIdentifier(node->result_name));
            }

            case ActionType::COLUMN:
                return emit(formatConstant(node));
            case ActionType::ALIAS:
                return recurse(node->children.front(), parent_precedence);

            case ActionType::ARRAY_JOIN:
                charge(std::string_view("arrayJoin()").size());
                return "arrayJoin(" + recurse(node->children.front(), 0) + ")";

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
                            return emit(fmt::format("{}({}, {})", pretty_filter_name, probe_column, build_column));
                        return emit(fmt::format("{}({}, {} from {})", pretty_filter_name, probe_column, build_column, build_table));
                    }
                    return emit(fmt::format("{}({})", filter_id, probe_column));
                }

                if ((func_name == "_CAST" || func_name == "CAST") && node->children.size() == 2)
                {
                    Field type_field;
                    node->children[1]->column->get(0, type_field);
                    auto type_name = type_field.safeGet<String>();
                    charge(std::string_view("CAST( AS )").size() + type_name.size());
                    auto inner = recurse(node->children[0], 0);
                    return "CAST(" + inner + " AS " + type_name + ")";
                }

                auto op_info = getOperatorInfo(func_name);

                if (func_name == "not" && node->children.size() == 1)
                {
                    charge(std::string_view("NOT ").size());
                    String result = "NOT " + recurse(node->children[0], op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "negate" && node->children.size() == 1)
                {
                    charge(1);
                    String result = "-" + recurse(node->children[0], op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "isNull" && node->children.size() == 1)
                {
                    charge(std::string_view(" IS NULL").size());
                    return recurse(node->children[0], op_info->precedence) + " IS NULL";
                }

                if (func_name == "isNotNull" && node->children.size() == 1)
                {
                    charge(std::string_view(" IS NOT NULL").size());
                    return recurse(node->children[0], op_info->precedence) + " IS NOT NULL";
                }

                if ((func_name == "and" || func_name == "or") && node->children.size() >= 2)
                {
                    String separator = fmt::format(" {} ", op_info->symbol);
                    std::vector<String> parts;
                    parts.reserve(node->children.size());
                    for (const auto * child : node->children)
                    {
                        if (!parts.empty())
                            charge(separator.size());
                        parts.push_back(recurse(child, op_info->precedence));
                    }

                    String result = fmt::format("{}", fmt::join(parts, separator));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "arrayElement" && node->children.size() == 2)
                {
                    charge(2);
                    auto arr = recurse(node->children[0], op_info->precedence);
                    auto idx = recurse(node->children[1], 0);
                    return arr + "[" + idx + "]";
                }

                if (func_name == "tupleElement" && node->children.size() == 2)
                {
                    charge(1);
                    auto tup = recurse(node->children[0], op_info->precedence);
                    auto elem = recurse(node->children[1], 0);
                    return tup + "." + elem;
                }

                if (op_info && (op_info->symbol == "IN" || op_info->symbol == "NOT IN")
                    && node->children.size() == 2)
                {
                    auto lhs = recurse(node->children[0], op_info->precedence);
                    auto rhs = formatSetPretty(node->children[1], subquery_set_names);
                    charge(op_info->symbol.size() + 2 + rhs.size());
                    String result = fmt::format("{} {} {}", lhs, op_info->symbol, rhs);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (op_info && !op_info->symbol.empty() && node->children.size() == 2)
                {
                    charge(op_info->symbol.size() + 2);
                    auto lhs = recurse(node->children[0], op_info->precedence);
                    auto rhs = recurse(node->children[1], op_info->precedence);
                    String result = fmt::format("{} {} {}", lhs, op_info->symbol, rhs);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                charge(func_name.size() + 2);
                std::vector<String> args;
                args.reserve(node->children.size());
                for (const auto * child : node->children)
                {
                    if (!args.empty())
                        charge(2);
                    args.push_back(recurse(child, 0));
                }

                return func_name + "(" + fmt::format("{}", fmt::join(args, ", ")) + ")";
            }

            default:
                return emit(node->result_name);
        }
    }

    String formatNodePretty(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names,
        int parent_precedence)
    {
        LengthBudget budget;
        auto result = formatNodePrettyImpl(node, pretty_names, runtime_filter_names, subquery_set_names, parent_precedence, budget);
        clipToMaxLength(result);
        return result;
    }

    String formatColumnPretty(const String & column_name, const std::unordered_map<String, PrettyColumnName> & pretty_names)
    {
        if (auto it = pretty_names.find(column_name); it != pretty_names.end())
            return it->second.expression;
        return trimColumnIdentifier(column_name);
    }

    bool appendBounded(String & target, std::string_view text)
    {
        if (target.size() >= MAX_EXPRESSION_LENGTH)
            return false;

        if (target.size() + text.size() > MAX_EXPRESSION_LENGTH)
        {
            target.append(text, 0, MAX_EXPRESSION_LENGTH - target.size());
            target += TRUNCATED_MARKER;
            return false;
        }

        target += text;
        return true;
    }

    /// The budget stops the descent, but each subtree collapsed after it ran out still contributes its
    /// own marker, so clip afterwards to make the limit exact rather than approached from above.
    void clipToMaxLength(String & text)
    {
        if (text.size() > MAX_EXPRESSION_LENGTH)
        {
            text.resize(MAX_EXPRESSION_LENGTH);
            text += TRUNCATED_MARKER;
        }
    }

    static PrettyColumnName formatFilterPretty(
        const ActionsDAG & dag,
        const String & column_name,
        const std::unordered_map<String, PrettyColumnName> & pretty_names,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> & subquery_set_names)
    {
        const auto * root = dag.tryFindInOutputs(column_name);
        if (!root)
            return PrettyColumnName(trimColumnIdentifier(column_name));

        /// The split is bounded because it cannot be budgeted: it runs before anything is rendered, and
        /// it alone allocates one atom per path. The condition and the runtime-filter annotation are
        /// separate output lines, so one bound across both would erase whichever the walk reaches last.
        auto split = extractConjunctionAtomsBounded(root, MAX_CONJUNCTION_ATOMS_PER_CLASS);

        const auto render = [&](const ActionsDAG::NodeRawConstPtrs & atoms, bool dropped_atoms)
        {
            LengthBudget budget;
            std::vector<String> parts;
            bool truncated = dropped_atoms;
            for (const auto * atom : atoms)
            {
                if (budget.exhausted())
                {
                    truncated = true;
                    break;
                }
                if (!parts.empty())
                    budget.charge(std::string_view(" AND ").size());
                parts.push_back(
                    formatNodePrettyImpl(atom, pretty_names, runtime_filter_names, subquery_set_names, 4, budget));
            }
            return std::pair{std::move(parts), truncated};
        };

        auto [user_parts, user_truncated] = render(split.user_atoms, split.user_truncated);
        auto [rf_parts, rf_truncated] = render(split.runtime_filter_atoms, split.runtime_filter_truncated);

        String expression;
        bool expression_truncated = user_truncated;
        if (!user_parts.empty())
            expression = fmt::format(" {}", fmt::join(user_parts, " AND "));
        /// A condition left unreached is indistinguishable here from one that is absent, so name the
        /// filter column and mark it partial. The caller drops an empty expression entirely, and a
        /// dropped line reads as "there is no condition" rather than "there is more".
        if (expression.empty() && (rf_parts.empty() || split.user_atom_unreached))
        {
            expression = fmt::format(" {}", trimColumnIdentifier(column_name));
            expression_truncated |= split.user_atom_unreached;
        }
        if (expression_truncated)
            expression += fmt::format(" {}", TRUNCATED_MARKER);

        /// Marked even once nothing nameable is left, so an annotation cut short does not read as absent.
        String annotation;
        if (!rf_parts.empty() || rf_truncated)
        {
            annotation = fmt::format("Runtime filters:{}{}",
                rf_parts.empty() ? "" : " ", fmt::join(rf_parts, " AND "));
            if (rf_truncated)
                annotation += fmt::format(" {}", TRUNCATED_MARKER);
        }

        clipToMaxLength(expression);
        clipToMaxLength(annotation);

        return {std::move(expression), std::move(annotation)};
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

            /// Each argument is bounded on its own, so k of them concatenate to k times the cap.
            pretty += '(';
            bool first = true;
            for (const auto & arg : agg.argument_names)
            {
                if (!first && !appendBounded(pretty, ", "))
                    break;
                first = false;
                if (auto it = pretty_names.find(arg); it != pretty_names.end())
                {
                    if (!appendBounded(pretty, it->second.expression))
                        break;
                }
                else if (!appendBounded(pretty, trimColumnIdentifier(arg)))
                    break;
            }
            pretty += ')';
            clipToMaxLength(pretty);
            pretty_names.try_emplace(agg.column_name, PrettyColumnName(std::move(pretty)));
        }
    }

    static void addWindowFunctionPrettyNames(const WindowDescription & window_description, std::unordered_map<String, PrettyColumnName> & pretty_names)
    {
        /// Each rendered column name is bounded on its own, so k of them compose to k times the cap.
        String spec = "(";

        if (!window_description.partition_by.empty())
        {
            spec += "PARTITION BY ";
            for (size_t i = 0; i < window_description.partition_by.size(); ++i)
            {
                if (i > 0 && !appendBounded(spec, ", "))
                    break;
                if (!appendBounded(spec, formatColumnPretty(window_description.partition_by[i].column_name, pretty_names)))
                    break;
            }
        }

        if (!window_description.partition_by.empty() && !window_description.order_by.empty())
            appendBounded(spec, " ");

        if (!window_description.order_by.empty())
        {
            appendBounded(spec, "ORDER BY ");
            for (size_t i = 0; i < window_description.order_by.size(); ++i)
            {
                if (i > 0 && !appendBounded(spec, ", "))
                    break;
                const auto & desc = window_description.order_by[i];
                if (!appendBounded(spec, formatColumnPretty(desc.column_name, pretty_names)))
                    break;
                if (!appendBounded(spec, desc.direction > 0 ? " ASC" : " DESC"))
                    break;
                if (desc.with_fill && !appendBounded(spec, " WITH FILL"))
                    break;
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
                if (i > 0 && !appendBounded(pretty, ", "))
                    break;
                if (!appendBounded(pretty, formatColumnPretty(func.argument_names[i], pretty_names)))
                    break;
            }
            pretty += ") OVER ";
            pretty += spec;
            clipToMaxLength(pretty);

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

    using PerPlanColumnMaps = std::unordered_map<const QueryPlan *, PrettyColumnNameMap>;

    static void buildPrettyNamesForOneNode(
        const QueryPlan::Node * node,
        PrettyColumnNameMap & pretty_names,
        PrettyRuntimeFilterNameMap & runtime_filter_names,
        PrettySetNameMap & subquery_set_names)
    {
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
        else if (step_name == "Aggregating")
        {
            addAggregatesPrettyNames(static_cast<const AggregatingStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "AggregatingProjection")
        {
            addAggregatesPrettyNames(static_cast<const AggregatingProjectionStep *>(step.get())->getParams(), pretty_names);
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

    /// Names a node's children and child plans before the node itself, so every node sees its inputs as
    /// already-rendered names. The traversal is iterative because plan depth is not bounded by the query
    /// text: a join builds one nested runtime-filter node per key.
    static void buildPrettyNamesForNode(
        const QueryPlan::Node * root,
        PrettyColumnNameMap & root_columns,
        PrettyRuntimeFilterNameMap & runtime_filter_names,
        PrettySetNameMap & subquery_set_names,
        PerPlanColumnMaps & per_plan_columns)
    {
        struct Frame
        {
            const QueryPlan::Node * node;
            PrettyColumnNameMap * columns;
            size_t children_taken = 0;
            bool child_plans_taken = false;
        };

        std::vector<Frame> stack;
        stack.push_back({root, &root_columns});

        while (!stack.empty())
        {
            const auto * node = stack.back().node;
            auto * columns = stack.back().columns;

            /// One child per iteration, last to first, matching the order each name is first defined in.
            if (stack.back().children_taken < node->children.size())
            {
                const auto * child = node->children[node->children.size() - 1 - stack.back().children_taken];
                ++stack.back().children_taken;
                stack.push_back({child, columns});
                continue;
            }

            if (!stack.back().child_plans_taken)
            {
                stack.back().child_plans_taken = true;
                /// A child plan is a separate naming scope. Build its column names into their own map so
                /// the parent sees the child's output columns as leaves (trimmed identifiers) rather than
                /// the child's internal expressions; otherwise nested plans compound the rendering, e.g.
                /// `materialize(materialize(...))`. Runtime-filter and subquery-set names are global ids,
                /// so they stay shared across the whole tree.
                auto child_plans = node->step->getChildPlans();
                /// Pushed back to front so they come off the stack front to back.
                for (auto it = child_plans.rbegin(); it != child_plans.rend(); ++it)
                    if (*it && (*it)->getRootNode())
                        stack.push_back({(*it)->getRootNode(), &per_plan_columns[*it]});
                continue;
            }

            stack.pop_back();
            buildPrettyNamesForOneNode(node, *columns, runtime_filter_names, subquery_set_names);
        }
    }

    PrettyNamesPerPlan buildPrettyNamesPerPlan(const QueryPlan & plan)
    {
        /// Runtime-filter and subquery-set names are global ids; keep them shared across the whole tree
        /// so their numbering stays consistent regardless of plan boundaries. Only column names are scoped.
        PrettyRuntimeFilterNameMap runtime_filter_names;
        PrettySetNameMap subquery_set_names;
        PerPlanColumnMaps per_plan_columns;

        /// Reserve the top plan's slot first so it is keyed even if it has no expression columns.
        auto & top_columns = per_plan_columns[&plan];
        auto * root = plan.getRootNode();
        if (root)
            buildPrettyNamesForNode(root, top_columns, runtime_filter_names, subquery_set_names, per_plan_columns);

        PrettyNamesPerPlan result;
        for (auto & [plan_ptr, columns] : per_plan_columns)
        {
            /// Subquery-set names are global; expose them in every plan's map so set references resolve.
            for (const auto & [hash, name] : subquery_set_names)
                columns[PreparedSets::toString(hash, {})] = PrettyColumnName(name);
            result.names.emplace(plan_ptr, PrettyNames{std::move(columns), runtime_filter_names});
        }
        return result;
    }

}

}
