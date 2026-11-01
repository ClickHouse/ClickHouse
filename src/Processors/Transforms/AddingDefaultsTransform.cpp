#include <Common/typeid_cast.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/FilterDescription.h>

#include <Core/callOnTypeIndex.h>
#include <Core/BlockMissingValues.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}


static void checkCalculated(const ColumnWithTypeAndName & col_read,
                            const ColumnWithTypeAndName & col_defaults,
                            size_t defaults_needed)
{
    size_t column_size = col_read.column->size();

    if (column_size != col_defaults.column->size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Mismatch column sizes while adding defaults");

    if (column_size < defaults_needed)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Unexpected defaults count");

    if (!col_read.type->equals(*col_defaults.type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Mismatch column types while adding defaults");
}

static void mixNumberColumns(
    TypeIndex type_idx,
    MutableColumnPtr & column_mixed,
    const ColumnPtr & col_defaults,
    const BlockMissingValues::RowsBitMask & defaults_mask)
{
    auto call = [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using DataType = typename Types::LeftType;

        if constexpr (!std::is_same_v<DataType, DataTypeString> && !std::is_same_v<DataType, DataTypeFixedString>)
        {
            using FieldType = typename DataType::FieldType;
            using ColVecType = ColumnVectorOrDecimal<FieldType>;

            auto col_read = typeid_cast<ColVecType *>(column_mixed.get());
            if (!col_read)
                return false;

            typename ColVecType::Container & dst = col_read->getData();

            if (auto const_col_defs = checkAndGetColumnConst<ColVecType>(col_defaults.get()))
            {
                FieldType value = checkAndGetColumn<ColVecType>(const_col_defs->getDataColumnPtr().get())->getData()[0];

                for (size_t i = 0; i < defaults_mask.size(); ++i)
                    if (defaults_mask[i])
                        dst[i] = value;

                return true;
            }
            if (auto col_defs = checkAndGetColumn<ColVecType>(col_defaults.get()))
            {
                auto & src = col_defs->getData();
                for (size_t i = 0; i < defaults_mask.size(); ++i)
                    if (defaults_mask[i])
                        dst[i] = src[i];

                return true;
            }
        }

        return false;
    };

    if (!callOnIndexAndDataType<void>(type_idx, call))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type on mixNumberColumns");
}

static MutableColumnPtr mixColumns(
    const ColumnWithTypeAndName & col_read,
    const ColumnWithTypeAndName & col_defaults,
    const BlockMissingValues::RowsBitMask & defaults_mask)
{
    size_t column_size = col_read.column->size();
    size_t defaults_needed = defaults_mask.size();

    MutableColumnPtr column_mixed = col_read.column->cloneEmpty();

    for (size_t i = 0; i < defaults_needed; ++i)
    {
        if (defaults_mask[i])
        {
            if (isColumnConst(*col_defaults.column))
                column_mixed->insert((*col_defaults.column)[i]);
            else
                column_mixed->insertFrom(*col_defaults.column, i);
        }
        else
            column_mixed->insertFrom(*col_read.column, i);
    }

    for (size_t i = defaults_needed; i < column_size; ++i)
        column_mixed->insertFrom(*col_read.column, i);

    return column_mixed;
}


/// The column which a name required by a default expression refers to, e.g. `a` for a subcolumn `a.x` of a tuple.
/// `columns` may have no subcolumns, so it is found by the type of the column.
static String getRequiredColumnName(const String & required_name, const ColumnsDescription & columns)
{
    if (columns.has(required_name))
        return required_name;

    for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(required_name))
        if (columns.has(String(column_name)) && columns.get(String(column_name)).type->hasSubcolumn(subcolumn_name))
            return String(column_name);

    return required_name;
}

AddingDefaultsTransform::AddingDefaultsTransform(
    SharedHeader header,
    const ColumnsDescription & columns_,
    IInputFormat & input_format_,
    ContextPtr context_,
    const std::optional<NamesAndTypesList> & needed_columns_)
    : ISimpleTransform(header, header, true)
    , columns(columns_)
    , column_defaults(columns.getDefaults())
    , input_format(input_format_)
    , context(context_)
{
    if (needed_columns_)
    {
        needed_columns.emplace();
        for (const auto & column : *needed_columns_)
            needed_columns->insert(column.getNameInStorage());
    }
}


void AddingDefaultsTransform::onCancel() noexcept
{
    ISimpleTransform::onCancel();

    ExpressionActionsPtr actions;
    {
        std::lock_guard lock(current_actions_mutex);
        actions = current_actions;
    }

    if (!actions)
        return;

    for (const auto & node : actions->getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
            node.function->cancelExecution();
    }
}


void AddingDefaultsTransform::transform(Chunk & chunk)
{
    if (column_defaults.empty())
        return;

    const auto * block_missing_values = input_format.getMissingValues();
    if (!block_missing_values)
        return;

    const auto & header = getOutputPort().getHeader();
    size_t num_rows = chunk.getNumRows();
    auto res = header.cloneWithColumns(chunk.detachColumns());

    /// Identify columns that need defaults computed
    std::vector<std::pair<String, size_t>> columns_needing_defaults;
    for (const auto & [col_name, col_default] : column_defaults)
    {
        if (!res.has(col_name))
            continue;
        size_t column_idx = res.getPositionByName(col_name);
        if (block_missing_values->hasDefaultBits(column_idx))
            columns_needing_defaults.emplace_back(col_name, column_idx);
    }

    /// The columns which are read only for the defaults of the needed columns: their own defaults are computed only
    /// if a default which is computed depends on them. Otherwise they could fail on the values which are not used.
    if (needed_columns && !columns_needing_defaults.empty())
    {
        NameSet missing_columns;
        for (const auto & [col_name, col_idx] : columns_needing_defaults)
            missing_columns.insert(col_name);

        NameSet columns_to_compute;
        NameSet visited;
        std::vector<String> names_to_visit;
        for (const auto & [col_name, col_idx] : columns_needing_defaults)
            if (needed_columns->contains(col_name))
                names_to_visit.push_back(col_name);

        while (!names_to_visit.empty())
        {
            const String name = std::move(names_to_visit.back());
            names_to_visit.pop_back();

            if (!visited.insert(name).second)
                continue;

            /// The default of a column which is read and has no missing values is not used.
            if (missing_columns.contains(name))
                columns_to_compute.insert(name);
            else if (res.has(name))
                continue;

            auto it = column_defaults.find(name);
            if (it == column_defaults.end())
                continue;

            RequiredSourceColumnsVisitor::Data columns_context;
            auto expression = it->second.expression->clone();
            RequiredSourceColumnsVisitor(columns_context).visit(expression);

            for (const auto & required_name : columns_context.requiredColumns())
                names_to_visit.push_back(getRequiredColumnName(required_name, columns));
        }

        std::erase_if(columns_needing_defaults, [&](const auto & column) { return !columns_to_compute.contains(column.first); });
    }

    if (columns_needing_defaults.empty())
    {
        chunk.setColumns(res.getColumns(), num_rows);
        return;
    }

    /// Build dependency graph: for each column needing defaults, find which other
    /// columns (that also need defaults) its default expression depends on.
    /// This is needed because if column `s` has DEFAULT concat('test', CAST(n, 'String'))
    /// and column `n` has DEFAULT 42, when inserting {"n": 2} we need to first compute
    /// n's defaults (for rows where n is missing), mix them into the block, and only
    /// then compute s's defaults so that s can see the correct values of n.
    std::unordered_map<String, NameSet> dependencies;
    /// The dependencies by the required names as they are, without resolving subcolumns to their columns:
    /// they order a cycle which exists only through a subcolumn, e.g. `a DEFAULT tuple(b)` and `b DEFAULT a.x`, as before.
    std::unordered_map<String, NameSet> raw_dependencies;
    std::unordered_set<String> columns_needing_defaults_set;
    for (const auto & [col_name, col_idx] : columns_needing_defaults)
        columns_needing_defaults_set.insert(col_name);

    for (const auto & [col_name, col_idx] : columns_needing_defaults)
    {
        const auto & col_default = column_defaults.at(col_name);
        RequiredSourceColumnsVisitor::Data columns_context;
        auto expr_clone = col_default.expression->clone();
        RequiredSourceColumnsVisitor(columns_context).visit(expr_clone);
        NameSet required = columns_context.requiredColumns();

        NameSet deps;
        NameSet raw_deps;
        for (const auto & req : required)
        {
            if (columns_needing_defaults_set.contains(req))
                raw_deps.insert(req);
            if (auto name = getRequiredColumnName(req, columns); columns_needing_defaults_set.contains(name))
                deps.insert(std::move(name));
        }
        dependencies[col_name] = std::move(deps);
        raw_dependencies[col_name] = std::move(raw_deps);
    }

    /// Process columns in dependency order (topological sort).
    /// In each iteration, process columns whose dependencies have all been satisfied.
    std::unordered_set<String> processed;
    std::unordered_map<size_t, MutableColumnPtr> mixed_columns;

    while (processed.size() < columns_needing_defaults.size())
    {
        /// Find columns ready to process (all dependencies satisfied)
        auto find_ready = [&](std::unordered_map<String, NameSet> & dependencies_to_check)
        {
            std::vector<std::pair<String, size_t>> result;
            for (const auto & [col_name, col_idx] : columns_needing_defaults)
            {
                if (processed.contains(col_name))
                    continue;

                bool all_deps_ready = true;
                for (const auto & dep : dependencies_to_check[col_name])
                {
                    if (!processed.contains(dep))
                    {
                        all_deps_ready = false;
                        break;
                    }
                }
                if (all_deps_ready)
                    result.emplace_back(col_name, col_idx);
            }
            return result;
        };

        auto ready = find_ready(dependencies);
        if (ready.empty())
            ready = find_ready(raw_dependencies);

        if (ready.empty())
        {
            /// Circular dependency detected - process all remaining columns together
            /// to preserve old behavior for edge cases
            for (const auto & [col_name, col_idx] : columns_needing_defaults)
            {
                if (!processed.contains(col_name))
                    ready.emplace_back(col_name, col_idx);
            }
        }

        /// Build evaluate_block: start from res (which has updated values from previous iterations),
        /// then remove only the columns we're computing defaults for in this iteration
        Block evaluate_block{res};
        NamesAndTypesList required_columns_list;

        for (const auto & [col_name, col_idx] : ready)
        {
            if (evaluate_block.has(col_name))
                evaluate_block.erase(col_name);
            const auto & col = header.getByPosition(col_idx);
            required_columns_list.emplace_back(col.name, col.type);
        }

        if (!evaluate_block.columns())
            evaluate_block.insert({ColumnConst::create(ColumnUInt8::create(1, static_cast<UInt8>(0)), num_rows),
                                   std::make_shared<DataTypeUInt8>(), "_dummy"});

        /// Evaluate defaults for the ready columns
        auto dag = evaluateMissingDefaults(evaluate_block, required_columns_list, columns, context, false);
        if (dag)
        {
            auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(header, dag->getRequiredColumnsNames(), context);
            auto actions = std::make_shared<ExpressionActions>(
                ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(*dag)),
                ExpressionActionsSettings(context, CompileExpressions::yes), true);

            /// Publish the actions before anything can observe the cancellation, so that a
            /// concurrent `onCancel` can always forward `cancelExecution` into a function that is
            /// about to run or already running. Publishing after the check below would leave a
            /// window where `onCancel` sees no actions and the evaluation still starts one action.
            {
                std::lock_guard lock(current_actions_mutex);
                current_actions = actions;
            }

            /// The task can be dispatched before the query is cancelled and start running after it:
            /// skip the whole default evaluation instead of running one action of it. A cancellation
            /// landing after this check reaches the running function through the published actions,
            /// if it supports interruption.
            if (isCancelled())
            {
                {
                    std::lock_guard lock(current_actions_mutex);
                    current_actions.reset();
                }
                chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
                return;
            }

            actions->execute(evaluate_block, false, false, &getCancellationFlag());

            {
                std::lock_guard lock(current_actions_mutex);
                current_actions.reset();
            }

            if (isCancelled())
            {
                chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
                return;
            }
        }

        /// Mix the computed defaults back into res
        for (const auto & [col_name, col_idx] : ready)
        {
            if (!evaluate_block.has(col_name))
            {
                processed.insert(col_name);
                continue;
            }

            const auto & column_def = evaluate_block.getByName(col_name);
            ColumnWithTypeAndName & column_read = res.getByPosition(col_idx);
            const auto & defaults_mask = block_missing_values->getDefaultsBitmask(col_idx);

            checkCalculated(column_read, column_def, defaults_mask.size());

            if (!defaults_mask.empty())
            {
                column_read.column = removeSpecialRepresentations(column_read.column);
                auto column_def_cleaned = removeSpecialRepresentations(column_def.column);

                /// TODO: FixedString
                if (isColumnedAsNumber(column_read.type) || isDecimal(column_read.type))
                {
                    MutableColumnPtr column_mixed = IColumn::mutate(std::move(column_read.column));
                    mixNumberColumns(column_read.type->getTypeId(), column_mixed, column_def_cleaned, defaults_mask);
                    column_read.column = std::move(column_mixed);
                }
                else
                {
                    ColumnWithTypeAndName column_def_for_mix{column_def_cleaned, column_def.type, column_def.name};
                    MutableColumnPtr column_mixed = mixColumns(column_read, column_def_for_mix, defaults_mask);
                    mixed_columns.emplace(col_idx, std::move(column_mixed));
                }
            }

            processed.insert(col_name);
        }

        /// Apply non-numeric mixed columns to res after each iteration
        /// so they're available for the next iteration's evaluate_block
        if (!mixed_columns.empty())
        {
            MutableColumns mutation = res.mutateColumns();
            for (auto & [position, mixed_col] : mixed_columns)
                mutation[position] = std::move(mixed_col);
            res.setColumns(std::move(mutation));
            mixed_columns.clear();
        }
    }

    chunk.setColumns(res.getColumns(), num_rows);
}

}
