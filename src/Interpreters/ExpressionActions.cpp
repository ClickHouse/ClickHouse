#include <Interpreters/Set.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionJIT.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <optional>
#include <Columns/ColumnSet.h>
#include <Functions/FunctionHelpers.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace ProfileEvents
{
    extern const Event FunctionExecute;
    extern const Event CompiledFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
    extern const int TOO_MANY_TEMPORARY_NON_CONST_COLUMNS;
}

/// Read comment near usage
static constexpr auto DUMMY_COLUMN_NAME = "_dummy";


Names ExpressionAction::getNeededColumns() const
{
    Names res = argument_names;

    if (array_join)
        res.insert(res.end(), array_join->columns.begin(), array_join->columns.end());

    if (table_join)
        res.insert(res.end(), table_join->keyNamesLeft().begin(), table_join->keyNamesLeft().end());

    for (const auto & column : projection)
        res.push_back(column.first);

    if (!source_name.empty())
        res.push_back(source_name);

    return res;
}


ExpressionAction ExpressionAction::applyFunction(
    const FunctionOverloadResolverPtr & function_,
    const std::vector<std::string> & argument_names_,
    std::string result_name_)
{
    if (result_name_.empty())
    {
        result_name_ = function_->getName() + "(";
        for (size_t i = 0 ; i < argument_names_.size(); ++i)
        {
            if (i)
                result_name_ += ", ";
            result_name_ += argument_names_[i];
        }
        result_name_ += ")";
    }

    ExpressionAction a;
    a.type = APPLY_FUNCTION;
    a.result_name = result_name_;
    a.function_builder = function_;
    a.argument_names = argument_names_;
    return a;
}

ExpressionAction ExpressionAction::addColumn(
    const ColumnWithTypeAndName & added_column_)
{
    ExpressionAction a;
    a.type = ADD_COLUMN;
    a.result_name = added_column_.name;
    a.result_type = added_column_.type;
    a.added_column = added_column_.column;
    return a;
}

ExpressionAction ExpressionAction::removeColumn(const std::string & removed_name)
{
    ExpressionAction a;
    a.type = REMOVE_COLUMN;
    a.source_name = removed_name;
    return a;
}

ExpressionAction ExpressionAction::copyColumn(const std::string & from_name, const std::string & to_name, bool can_replace)
{
    ExpressionAction a;
    a.type = COPY_COLUMN;
    a.source_name = from_name;
    a.result_name = to_name;
    a.can_replace = can_replace;
    return a;
}

ExpressionAction ExpressionAction::project(const NamesWithAliases & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projection = projected_columns_;
    return a;
}

ExpressionAction ExpressionAction::project(const Names & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projection.resize(projected_columns_.size());
    for (size_t i = 0; i < projected_columns_.size(); ++i)
        a.projection[i] = NameWithAlias(projected_columns_[i], "");
    return a;
}

ExpressionAction ExpressionAction::addAliases(const NamesWithAliases & aliased_columns_)
{
    ExpressionAction a;
    a.type = ADD_ALIASES;
    a.projection = aliased_columns_;
    return a;
}

ExpressionAction ExpressionAction::arrayJoin(const NameSet & array_joined_columns, bool array_join_is_left, const Context & context)
{
    ExpressionAction a;
    a.type = ARRAY_JOIN;
    a.array_join = std::make_shared<ArrayJoinAction>(array_joined_columns, array_join_is_left, context);
    return a;
}

ExpressionAction ExpressionAction::ordinaryJoin(std::shared_ptr<TableJoin> table_join, JoinPtr join)
{
    ExpressionAction a;
    a.type = JOIN;
    a.table_join = table_join;
    a.join = join;
    return a;
}

void ExpressionAction::prepare(Block & sample_block, const Settings & settings, NameSet & names_not_for_constant_folding)
{
    // std::cerr << "preparing: " << toString() << std::endl;

    /** Constant expressions should be evaluated, and put the result in sample_block.
      */

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            if (sample_block.has(result_name))
                throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

            bool all_const = true;
            bool all_suitable_for_constant_folding = true;

            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                arguments[i] = sample_block.getPositionByName(argument_names[i]);
                ColumnPtr col = sample_block.safeGetByPosition(arguments[i]).column;
                if (!col || !isColumnConst(*col))
                    all_const = false;

                if (names_not_for_constant_folding.count(argument_names[i]))
                    all_suitable_for_constant_folding = false;
            }

            size_t result_position = sample_block.columns();
            sample_block.insert({nullptr, result_type, result_name});
            function = function_base->prepare(sample_block, arguments, result_position);
            function->createLowCardinalityResultCache(settings.max_threads);

            bool compile_expressions = false;
#if USE_EMBEDDED_COMPILER
            compile_expressions = settings.compile_expressions;
#endif
            /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
            /// But if we compile expressions compiled version of this function maybe placed in cache,
            /// so we don't want to unfold non deterministic functions
            if (all_const && function_base->isSuitableForConstantFolding() && (!compile_expressions || function_base->isDeterministic()))
            {
                function->execute(sample_block, arguments, result_position, sample_block.rows(), true);

                /// If the result is not a constant, just in case, we will consider the result as unknown.
                ColumnWithTypeAndName & col = sample_block.safeGetByPosition(result_position);
                if (!isColumnConst(*col.column))
                {
                    col.column = nullptr;
                }
                else
                {
                    /// All constant (literal) columns in block are added with size 1.
                    /// But if there was no columns in block before executing a function, the result has size 0.
                    /// Change the size to 1.

                    if (col.column->empty())
                        col.column = col.column->cloneResized(1);

                    if (!all_suitable_for_constant_folding)
                        names_not_for_constant_folding.insert(result_name);
                }
            }

            /// Some functions like ignore() or getTypeName() always return constant result even if arguments are not constant.
            /// We can't do constant folding, but can specify in sample block that function result is constant to avoid
            /// unnecessary materialization.
            auto & res = sample_block.getByPosition(result_position);
            if (!res.column && function_base->isSuitableForConstantFolding())
            {
                if (auto col = function_base->getResultIfAlwaysReturnsConstantAndHasArguments(sample_block, arguments))
                {
                    res.column = std::move(col);
                    names_not_for_constant_folding.insert(result_name);
                }
            }

            break;
        }

        case ARRAY_JOIN:
        {
            array_join->prepare(sample_block);
            break;
        }

        case JOIN:
        {
            table_join->addJoinedColumnsAndCorrectNullability(sample_block);
            break;
        }

        case PROJECT:
        {
            Block new_block;

            for (const auto & elem : projection)
            {
                const std::string & name = elem.first;
                const std::string & alias = elem.second;
                ColumnWithTypeAndName column = sample_block.getByName(name);
                if (!alias.empty())
                    column.name = alias;
                new_block.insert(std::move(column));
            }

            sample_block.swap(new_block);
            break;
        }

        case ADD_ALIASES:
        {
            for (const auto & elem : projection)
            {
                const std::string & name = elem.first;
                const std::string & alias = elem.second;
                const ColumnWithTypeAndName & column = sample_block.getByName(name);
                if (!alias.empty() && !sample_block.has(alias))
                    sample_block.insert({column.column, column.type, alias});
            }
            break;
        }

        case REMOVE_COLUMN:
        {
            sample_block.erase(source_name);
            break;
        }

        case ADD_COLUMN:
        {
            if (sample_block.has(result_name))
                throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

            sample_block.insert(ColumnWithTypeAndName(added_column, result_type, result_name));
            break;
        }

        case COPY_COLUMN:
        {
            const auto & source = sample_block.getByName(source_name);
            result_type = source.type;

            if (sample_block.has(result_name))
            {
                if (can_replace)
                {
                    auto & result = sample_block.getByName(result_name);
                    result.type = result_type;
                    result.column = source.column;
                }
                else
                    throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
            }
            else
                sample_block.insert(ColumnWithTypeAndName(source.column, result_type, result_name));

            break;
        }
    }
}

void ExpressionAction::execute(Block & block, ExtraBlockPtr & not_processed) const
{
    switch (type)
    {
        case JOIN:
            join->joinBlock(block, not_processed);
            break;

        default:
            throw Exception("Unexpected expression call", ErrorCodes::LOGICAL_ERROR);
    }
}

void ExpressionAction::execute(Block & block, bool dry_run) const
{
    size_t input_rows_count = block.rows();

    if (type == REMOVE_COLUMN || type == COPY_COLUMN)
        if (!block.has(source_name))
            throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    if (type == ADD_COLUMN || (type == COPY_COLUMN && !can_replace) || type == APPLY_FUNCTION)
        if (block.has(result_name))
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
                arguments[i] = block.getPositionByName(argument_names[i]);

            size_t num_columns_without_result = block.columns();
            block.insert({ nullptr, result_type, result_name});

            ProfileEvents::increment(ProfileEvents::FunctionExecute);
            if (is_function_compiled)
                ProfileEvents::increment(ProfileEvents::CompiledFunctionExecute);
            function->execute(block, arguments, num_columns_without_result, input_rows_count, dry_run);

            break;
        }

        case ARRAY_JOIN:
        {
            array_join->execute(block, dry_run);
            break;
        }

        case JOIN:
            throw Exception("Unexpected JOIN expression call", ErrorCodes::LOGICAL_ERROR);

        case PROJECT:
        {
            Block new_block;

            for (const auto & elem : projection)
            {
                const std::string & name = elem.first;
                const std::string & alias = elem.second;
                ColumnWithTypeAndName column = block.getByName(name);
                if (!alias.empty())
                    column.name = alias;
                new_block.insert(std::move(column));
            }

            block.swap(new_block);

            break;
        }

        case ADD_ALIASES:
        {
            for (const auto & elem : projection)
            {
                const std::string & name = elem.first;
                const std::string & alias = elem.second;
                const ColumnWithTypeAndName & column = block.getByName(name);
                if (!alias.empty() && !block.has(alias))
                    block.insert({column.column, column.type, alias});
            }
            break;
        }

        case REMOVE_COLUMN:
            block.erase(source_name);
            break;

        case ADD_COLUMN:
            block.insert({ added_column->cloneResized(input_rows_count), result_type, result_name });
            break;

        case COPY_COLUMN:
            if (can_replace && block.has(result_name))
            {
                auto & result = block.getByName(result_name);
                const auto & source = block.getByName(source_name);
                result.type = source.type;
                result.column = source.column;
            }
            else
            {
                const auto & source_column = block.getByName(source_name);
                block.insert({source_column.column, source_column.type, result_name});
            }

            break;
    }
}

void ExpressionAction::executeOnTotals(Block & block) const
{
    if (type != JOIN)
        execute(block, false);
    else
        join->joinTotals(block);
}


std::string ExpressionAction::toString() const
{
    std::stringstream ss;
    switch (type)
    {
        case ADD_COLUMN:
            ss << "ADD " << result_name << " "
                << (result_type ? result_type->getName() : "(no type)") << " "
                << (added_column ? added_column->getName() : "(no column)");
            break;

        case REMOVE_COLUMN:
            ss << "REMOVE " << source_name;
            break;

        case COPY_COLUMN:
            ss << "COPY " << result_name << " = " << source_name;
            if (can_replace)
                ss << " (can replace)";
            break;

        case APPLY_FUNCTION:
            ss << "FUNCTION " << result_name << " " << (is_function_compiled ? "[compiled] " : "")
                << (result_type ? result_type->getName() : "(no type)") << " = "
                << (function_base ? function_base->getName() : "(no function)") << "(";
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << argument_names[i];
            }
            ss << ")";
            break;

        case ARRAY_JOIN:
            ss << (array_join->is_left ? "LEFT " : "") << "ARRAY JOIN ";
            for (NameSet::const_iterator it = array_join->columns.begin(); it != array_join->columns.end(); ++it)
            {
                if (it != array_join->columns.begin())
                    ss << ", ";
                ss << *it;
            }
            break;

        case JOIN:
            ss << "JOIN ";
            for (NamesAndTypesList::const_iterator it = table_join->columnsAddedByJoin().begin();
                 it != table_join->columnsAddedByJoin().end(); ++it)
            {
                if (it != table_join->columnsAddedByJoin().begin())
                    ss << ", ";
                ss << it->name;
            }
            break;

        case PROJECT: [[fallthrough]];
        case ADD_ALIASES:
            ss << (type == PROJECT ? "PROJECT " : "ADD_ALIASES ");
            for (size_t i = 0; i < projection.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << projection[i].first;
                if (!projection[i].second.empty() && projection[i].second != projection[i].first)
                    ss << " AS " << projection[i].second;
            }
            break;
    }

    return ss.str();
}

ExpressionActions::ExpressionActions(const NamesAndTypesList & input_columns_, const Context & context_)
    : input_columns(input_columns_), settings(context_.getSettingsRef())
{
    for (const auto & input_elem : input_columns)
        sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));

#if USE_EMBEDDED_COMPILER
compilation_cache = context_.getCompiledExpressionCache();
#endif
}

/// For constant columns the columns themselves can be contained in `input_columns_`.
ExpressionActions::ExpressionActions(const ColumnsWithTypeAndName & input_columns_, const Context & context_)
    : settings(context_.getSettingsRef())
{
    for (const auto & input_elem : input_columns_)
    {
        input_columns.emplace_back(input_elem.name, input_elem.type);
        sample_block.insert(input_elem);
    }
#if USE_EMBEDDED_COMPILER
    compilation_cache = context_.getCompiledExpressionCache();
#endif
}

ExpressionActions::~ExpressionActions() = default;

void ExpressionActions::checkLimits(Block & block) const
{
    if (settings.max_temporary_columns && block.columns() > settings.max_temporary_columns)
        throw Exception("Too many temporary columns: " + block.dumpNames()
            + ". Maximum: " + settings.max_temporary_columns.toString(),
            ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS);

    if (settings.max_temporary_non_const_columns)
    {
        size_t non_const_columns = 0;
        for (size_t i = 0, size = block.columns(); i < size; ++i)
            if (block.safeGetByPosition(i).column && !isColumnConst(*block.safeGetByPosition(i).column))
                ++non_const_columns;

        if (non_const_columns > settings.max_temporary_non_const_columns)
        {
            std::stringstream list_of_non_const_columns;
            for (size_t i = 0, size = block.columns(); i < size; ++i)
                if (block.safeGetByPosition(i).column && !isColumnConst(*block.safeGetByPosition(i).column))
                    list_of_non_const_columns << "\n" << block.safeGetByPosition(i).name;

            throw Exception("Too many temporary non-const columns:" + list_of_non_const_columns.str()
                + ". Maximum: " + settings.max_temporary_non_const_columns.toString(),
                ErrorCodes::TOO_MANY_TEMPORARY_NON_CONST_COLUMNS);
        }
    }
}

void ExpressionActions::addInput(const ColumnWithTypeAndName & column)
{
    input_columns.emplace_back(column.name, column.type);
    sample_block.insert(column);
}

void ExpressionActions::addInput(const NameAndTypePair & column)
{
    addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));
}

void ExpressionActions::add(const ExpressionAction & action, Names & out_new_columns)
{
    addImpl(action, out_new_columns);
}

void ExpressionActions::add(const ExpressionAction & action)
{
    Names new_names;
    addImpl(action, new_names);
}

void ExpressionActions::addImpl(ExpressionAction action, Names & new_names)
{
    if (!action.result_name.empty())
        new_names.push_back(action.result_name);

    if (action.array_join)
        new_names.insert(new_names.end(), action.array_join->columns.begin(), action.array_join->columns.end());

    /// Compiled functions are custom functions and they don't need building
    if (action.type == ExpressionAction::APPLY_FUNCTION && !action.is_function_compiled)
    {
        if (sample_block.has(action.result_name))
            throw Exception("Column '" + action.result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

        ColumnsWithTypeAndName arguments(action.argument_names.size());
        for (size_t i = 0; i < action.argument_names.size(); ++i)
        {
            if (!sample_block.has(action.argument_names[i]))
                throw Exception("Unknown identifier: '" + action.argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
            arguments[i] = sample_block.getByName(action.argument_names[i]);
        }

        action.function_base = action.function_builder->build(arguments);
        action.result_type = action.function_base->getReturnType();
    }

    if (action.type == ExpressionAction::ADD_ALIASES)
        for (const auto & name_with_alias : action.projection)
            new_names.emplace_back(name_with_alias.second);

    action.prepare(sample_block, settings, names_not_for_constant_folding);
    actions.push_back(action);
}

void ExpressionActions::prependProjectInput()
{
    actions.insert(actions.begin(), ExpressionAction::project(getRequiredColumns()));
}

void ExpressionActions::prependArrayJoin(const ExpressionAction & action, const Block & sample_block_before)
{
    if (action.type != ExpressionAction::ARRAY_JOIN)
        throw Exception("ARRAY_JOIN action expected", ErrorCodes::LOGICAL_ERROR);

    NameSet array_join_set(action.array_join->columns.begin(), action.array_join->columns.end());
    for (auto & it : input_columns)
    {
        if (array_join_set.count(it.name))
        {
            array_join_set.erase(it.name);
            it.type = std::make_shared<DataTypeArray>(it.type);
        }
    }
    for (const std::string & name : array_join_set)
    {
        input_columns.emplace_back(name, sample_block_before.getByName(name).type);
        actions.insert(actions.begin(), ExpressionAction::removeColumn(name));
    }

    actions.insert(actions.begin(), action);
    optimizeArrayJoin();
}


bool ExpressionActions::popUnusedArrayJoin(const Names & required_columns, ExpressionAction & out_action)
{
    if (actions.empty() || actions.back().type != ExpressionAction::ARRAY_JOIN)
        return false;
    NameSet required_set(required_columns.begin(), required_columns.end());
    for (const std::string & name : actions.back().array_join->columns)
    {
        if (required_set.count(name))
            return false;
    }
    for (const std::string & name : actions.back().array_join->columns)
    {
        DataTypePtr & type = sample_block.getByName(name).type;
        type = std::make_shared<DataTypeArray>(type);
    }
    out_action = actions.back();
    actions.pop_back();
    return true;
}

void ExpressionActions::execute(Block & block, bool dry_run) const
{
    for (const auto & action : actions)
    {
        action.execute(block, dry_run);
        checkLimits(block);
    }
}

void ExpressionActions::execute(Block & block, ExtraBlockPtr & not_processed) const
{
    if (actions.size() != 1)
        throw Exception("Continuation over multiple expressions is not supported", ErrorCodes::LOGICAL_ERROR);

    actions[0].execute(block, not_processed);
    checkLimits(block);
}

bool ExpressionActions::hasJoinOrArrayJoin() const
{
    for (const auto & action : actions)
        if (action.type == ExpressionAction::JOIN || action.type == ExpressionAction::ARRAY_JOIN)
            return true;

    return false;
}

bool ExpressionActions::hasTotalsInJoin() const
{
    for (const auto & action : actions)
        if (action.table_join && action.join->hasTotals())
            return true;
    return false;
}

void ExpressionActions::executeOnTotals(Block & block) const
{
    /// If there is `totals` in the subquery for JOIN, but we do not have totals, then take the block with the default values instead of `totals`.
    if (!block)
    {
        if (hasTotalsInJoin())
        {
            for (const auto & name_and_type : input_columns)
            {
                auto column = name_and_type.type->createColumn();
                column->insertDefault();
                block.insert(ColumnWithTypeAndName(std::move(column), name_and_type.type, name_and_type.name));
            }
        }
        else
            return; /// There's nothing to JOIN.
    }

    for (const auto & action : actions)
        action.executeOnTotals(block);
}

std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
    std::optional<size_t> min_size;
    String res;

    for (const auto & column : columns)
    {
        /// @todo resolve evil constant
        size_t size = column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100;

        if (!min_size || size < *min_size)
        {
            min_size = size;
            res = column.name;
        }
    }

    if (!min_size)
        throw Exception("No available columns", ErrorCodes::LOGICAL_ERROR);

    return res;
}

void ExpressionActions::finalize(const Names & output_columns)
{
    NameSet final_columns;
    for (const auto & name : output_columns)
    {
        if (!sample_block.has(name))
            throw Exception("Unknown column: " + name + ", there are only columns "
                            + sample_block.dumpNames(), ErrorCodes::UNKNOWN_IDENTIFIER);
        final_columns.insert(name);
    }

#if USE_EMBEDDED_COMPILER
    /// This has to be done before removing redundant actions and inserting REMOVE_COLUMNs
    /// because inlining may change dependency sets.
    if (settings.compile_expressions)
        compileFunctions(actions, output_columns, sample_block, compilation_cache, settings.min_count_to_compile_expression);
#endif

    /// Which columns are needed to perform actions from the current to the last.
    NameSet needed_columns = final_columns;
    /// Which columns nobody will touch from the current action to the last.
    NameSet unmodified_columns;

    {
        NamesAndTypesList sample_columns = sample_block.getNamesAndTypesList();
        for (const auto & sample_column : sample_columns)
            unmodified_columns.insert(sample_column.name);
    }

    /// Let's go from the end and maintain set of required columns at this stage.
    /// We will throw out unnecessary actions, although usually they are absent by construction.
    for (int i = static_cast<int>(actions.size()) - 1; i >= 0; --i)
    {
        ExpressionAction & action = actions[i];
        Names in = action.getNeededColumns();

        if (action.type == ExpressionAction::PROJECT)
        {
            needed_columns = NameSet(in.begin(), in.end());
            unmodified_columns.clear();
        }
        else if (action.type == ExpressionAction::ADD_ALIASES)
        {
            needed_columns.insert(in.begin(), in.end());
            for (auto & name_wit_alias : action.projection)
            {
                auto it = unmodified_columns.find(name_wit_alias.second);
                if (it != unmodified_columns.end())
                    unmodified_columns.erase(it);
            }
        }
        else if (action.type == ExpressionAction::ARRAY_JOIN)
        {
            action.array_join->finalize(needed_columns, unmodified_columns, final_columns);
        }
        else
        {
            std::string out = action.result_name;
            if (!out.empty())
            {
                /// If the result is not used and there are no side effects, throw out the action.
                if (!needed_columns.count(out) &&
                    (action.type == ExpressionAction::APPLY_FUNCTION
                    || action.type == ExpressionAction::ADD_COLUMN
                    || action.type == ExpressionAction::COPY_COLUMN))
                {
                    actions.erase(actions.begin() + i);

                    if (unmodified_columns.count(out))
                    {
                        sample_block.erase(out);
                        unmodified_columns.erase(out);
                    }

                    continue;
                }

                unmodified_columns.erase(out);
                needed_columns.erase(out);

                /** If the function is a constant expression, then replace the action by adding a column-constant - result.
                  * That is, we perform constant folding.
                  */
                if (action.type == ExpressionAction::APPLY_FUNCTION && sample_block.has(out))
                {
                    auto & result = sample_block.getByName(out);
                    if (result.column && names_not_for_constant_folding.count(result.name) == 0)
                    {
                        action.type = ExpressionAction::ADD_COLUMN;
                        action.result_type = result.type;
                        action.added_column = result.column;
                        action.function_builder = nullptr;
                        action.function_base = nullptr;
                        action.function = nullptr;
                        action.argument_names.clear();
                        in.clear();
                    }
                }
            }

            needed_columns.insert(in.begin(), in.end());
        }
    }


    /// 1) Sometimes we don't need any columns to perform actions and sometimes actions doesn't produce any columns as result.
    /// But Block class doesn't store any information about structure itself, it uses information from column.
    /// If we remove all columns from input or output block we will lose information about amount of rows in it.
    /// To avoid this situation we always leaving one of the columns in required columns (input)
    /// and output column. We choose that "redundant" column by size with help of getSmallestColumn.
    ///
    /// 2) Sometimes we have to read data from different Storages to execute query.
    /// For example in 'remote' function which requires to read data from local table (for example MergeTree) and
    /// remote table (doesn't know anything about it).
    ///
    /// If we have combination of two previous cases, our heuristic from (1) can choose absolutely different columns,
    /// so generated streams with these actions will have different headers. To avoid this we additionally rename our "redundant" column
    /// to DUMMY_COLUMN_NAME with help of COPY_COLUMN action and consequent remove of original column.
    /// It doesn't affect any logic, but all streams will have same "redundant" column in header called "_dummy".

    /// Also, it seems like we will always have same type (UInt8) of "redundant" column, but it's not obvious.

    bool dummy_column_copied = false;


    /// We will not throw out all the input columns, so as not to lose the number of rows in the block.
    if (needed_columns.empty() && !input_columns.empty())
    {
        auto colname = getSmallestColumn(input_columns);
        needed_columns.insert(colname);
        actions.insert(actions.begin(), ExpressionAction::copyColumn(colname, DUMMY_COLUMN_NAME, true));
        dummy_column_copied = true;
    }

    /// We will not leave the block empty so as not to lose the number of rows in it.
    if (final_columns.empty() && !input_columns.empty())
    {
        auto colname = getSmallestColumn(input_columns);
        final_columns.insert(DUMMY_COLUMN_NAME);
        if (!dummy_column_copied) /// otherwise we already have this column
            actions.insert(actions.begin(), ExpressionAction::copyColumn(colname, DUMMY_COLUMN_NAME, true));
    }

    for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end();)
    {
        NamesAndTypesList::iterator it0 = it;
        ++it;
        if (!needed_columns.count(it0->name))
        {
            if (unmodified_columns.count(it0->name))
                sample_block.erase(it0->name);
            input_columns.erase(it0);
        }
    }

/*    std::cerr << "\n";
      for (const auto & action : actions)
          std::cerr << action.toString() << "\n";
      std::cerr << "\n";*/

    /// Deletes unnecessary temporary columns.

    /// If the column after performing the function `refcount = 0`, it can be deleted.
    std::map<String, int> columns_refcount;

    for (const auto & name : final_columns)
        ++columns_refcount[name];

    for (const auto & action : actions)
    {
        if (!action.source_name.empty())
            ++columns_refcount[action.source_name];

        for (const auto & name : action.argument_names)
            ++columns_refcount[name];

        for (const auto & name_alias : action.projection)
            ++columns_refcount[name_alias.first];
    }

    Actions new_actions;
    new_actions.reserve(actions.size());

    for (const auto & action : actions)
    {
        new_actions.push_back(action);

        auto process = [&] (const String & name)
        {
            auto refcount = --columns_refcount[name];
            if (refcount <= 0)
            {
                new_actions.push_back(ExpressionAction::removeColumn(name));
                if (sample_block.has(name))
                    sample_block.erase(name);
            }
        };

        if (!action.source_name.empty())
            process(action.source_name);

        for (const auto & name : action.argument_names)
            process(name);

        /// For `projection`, there is no reduction in `refcount`, because the `project` action replaces the names of the columns, in effect, already deleting them under the old names.
    }

    actions.swap(new_actions);

/*    std::cerr << "\n";
    for (const auto & action : actions)
        std::cerr << action.toString() << "\n";
    std::cerr << "\n";*/

    optimizeArrayJoin();
    checkLimits(sample_block);
}


std::string ExpressionActions::dumpActions() const
{
    std::stringstream ss;

    ss << "input:\n";
    for (const auto & input_column : input_columns)
        ss << input_column.name << " " << input_column.type->getName() << "\n";

    ss << "\nactions:\n";
    for (const auto & action : actions)
        ss << action.toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (const auto & output_column : output_columns)
        ss << output_column.name << " " << output_column.type->getName() << "\n";

    return ss.str();
}

void ExpressionActions::optimizeArrayJoin()
{
    const size_t none = actions.size();
    size_t first_array_join = none;

    /// Columns that need to be evaluated for arrayJoin.
    /// Actions for adding them can not be moved to the left of the arrayJoin.
    NameSet array_joined_columns;

    /// Columns needed to evaluate arrayJoin or those that depend on it.
    /// Actions to delete them can not be moved to the left of the arrayJoin.
    NameSet array_join_dependencies;

    for (size_t i = 0; i < actions.size(); ++i)
    {
        /// Do not move the action to the right of the projection (the more that they are not usually there).
        if (actions[i].type == ExpressionAction::PROJECT)
            break;

        bool depends_on_array_join = false;
        Names needed;

        if (actions[i].type == ExpressionAction::ARRAY_JOIN)
        {
            depends_on_array_join = true;
            needed = actions[i].getNeededColumns();
        }
        else
        {
            if (first_array_join == none)
                continue;

            needed = actions[i].getNeededColumns();

            for (const auto & elem : needed)
            {
                if (array_joined_columns.count(elem))
                {
                    depends_on_array_join = true;
                    break;
                }
            }
        }

        if (depends_on_array_join)
        {
            if (first_array_join == none)
                first_array_join = i;

            if (!actions[i].result_name.empty())
                array_joined_columns.insert(actions[i].result_name);
            if (actions[i].array_join)
                array_joined_columns.insert(actions[i].array_join->columns.begin(), actions[i].array_join->columns.end());

            array_join_dependencies.insert(needed.begin(), needed.end());
        }
        else
        {
            bool can_move = false;

            if (actions[i].type == ExpressionAction::REMOVE_COLUMN)
            {
                /// If you delete a column that is not needed for arrayJoin (and those who depend on it), you can delete it before arrayJoin.
                can_move = !array_join_dependencies.count(actions[i].source_name);
            }
            else
            {
                /// If the action does not delete the columns and does not depend on the result of arrayJoin, you can make it until arrayJoin.
                can_move = true;
            }

            /// Move the current action to the position just before the first arrayJoin.
            if (can_move)
            {
                /// Move the i-th element to the position `first_array_join`.
                std::rotate(actions.begin() + first_array_join, actions.begin() + i, actions.begin() + i + 1);
                ++first_array_join;
            }
        }
    }
}


JoinPtr ExpressionActions::getTableJoinAlgo() const
{
    for (const auto & action : actions)
        if (action.join)
            return action.join;
    return {};
}


bool ExpressionActions::resultIsAlwaysEmpty() const
{
    /// Check that has join which returns empty result.

    for (const auto & action : actions)
    {
        if (action.type == action.JOIN && action.join && action.join->alwaysReturnsEmptySet())
            return true;
    }

    return false;
}


bool ExpressionActions::checkColumnIsAlwaysFalse(const String & column_name) const
{
    /// Check has column in (empty set).
    String set_to_check;

    for (auto it = actions.rbegin(); it != actions.rend(); ++it)
    {
        const auto & action = *it;
        if (action.type == action.APPLY_FUNCTION && action.function_base)
        {
            auto name = action.function_base->getName();
            if ((name == "in" || name == "globalIn")
                && action.result_name == column_name
                && action.argument_names.size() > 1)
            {
                set_to_check = action.argument_names[1];
                break;
            }
        }
    }

    if (!set_to_check.empty())
    {
        for (const auto & action : actions)
        {
            if (action.type == action.ADD_COLUMN && action.result_name == set_to_check)
            {
                // Constant ColumnSet cannot be empty, so we only need to check non-constant ones.
                if (const auto * column_set = checkAndGetColumn<const ColumnSet>(action.added_column.get()))
                {
                    if (column_set->getData()->isCreated() && column_set->getData()->getTotalRowCount() == 0)
                        return true;
                }
            }
        }
    }

    return false;
}


/// It is not important to calculate the hash of individual strings or their concatenation
UInt128 ExpressionAction::ActionHash::operator()(const ExpressionAction & action) const
{
    SipHash hash;
    hash.update(action.type);
    hash.update(action.is_function_compiled);
    switch (action.type)
    {
        case ADD_COLUMN:
            hash.update(action.result_name);
            if (action.result_type)
                hash.update(action.result_type->getName());
            if (action.added_column)
                hash.update(action.added_column->getName());
            break;
        case REMOVE_COLUMN:
            hash.update(action.source_name);
            break;
        case COPY_COLUMN:
            hash.update(action.result_name);
            hash.update(action.source_name);
            break;
        case APPLY_FUNCTION:
            hash.update(action.result_name);
            if (action.result_type)
                hash.update(action.result_type->getName());
            if (action.function_base)
            {
                hash.update(action.function_base->getName());
                for (const auto & arg_type : action.function_base->getArgumentTypes())
                    hash.update(arg_type->getName());
            }
            for (const auto & arg_name : action.argument_names)
                hash.update(arg_name);
            break;
        case ARRAY_JOIN:
            hash.update(action.array_join->is_left);
            for (const auto & col : action.array_join->columns)
                hash.update(col);
            break;
        case JOIN:
            for (const auto & col : action.table_join->columnsAddedByJoin())
                hash.update(col.name);
            break;
        case PROJECT:
            for (const auto & pair_of_strs : action.projection)
            {
                hash.update(pair_of_strs.first);
                hash.update(pair_of_strs.second);
            }
            break;
        case ADD_ALIASES:
            break;
    }
    UInt128 result;
    hash.get128(result.low, result.high);
    return result;
}

bool ExpressionAction::operator==(const ExpressionAction & other) const
{
    if (result_type != other.result_type)
    {
        if (result_type == nullptr || other.result_type == nullptr)
            return false;
        else if (!result_type->equals(*other.result_type))
            return false;
    }

    if (function_base != other.function_base)
    {
        if (function_base == nullptr || other.function_base == nullptr)
            return false;
        else if (function_base->getName() != other.function_base->getName())
            return false;

        const auto & my_arg_types = function_base->getArgumentTypes();
        const auto & other_arg_types = other.function_base->getArgumentTypes();
        if (my_arg_types.size() != other_arg_types.size())
            return false;

        for (size_t i = 0; i < my_arg_types.size(); ++i)
            if (!my_arg_types[i]->equals(*other_arg_types[i]))
                return false;
    }

    if (added_column != other.added_column)
    {
        if (added_column == nullptr || other.added_column == nullptr)
            return false;
        else if (added_column->getName() != other.added_column->getName())
            return false;
    }

    bool same_array_join = !array_join && !other.array_join;
    if (array_join && other.array_join)
        same_array_join = (array_join->columns == other.array_join->columns) &&
            (array_join->is_left == other.array_join->is_left);

    return source_name == other.source_name
        && result_name == other.result_name
        && argument_names == other.argument_names
        && same_array_join
        && TableJoin::sameJoin(table_join.get(), other.table_join.get())
        && projection == other.projection
        && is_function_compiled == other.is_function_compiled;
}

void ExpressionActionsChain::addStep()
{
    if (steps.empty())
        throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumnsWithTypeAndName();
    steps.push_back(Step(std::make_shared<ExpressionActions>(columns, context)));
}

void ExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        Names required_output = steps[i].required_output;
        std::unordered_map<String, size_t> required_output_indexes;
        for (size_t j = 0; j < required_output.size(); ++j)
            required_output_indexes[required_output[j]] = j;
        auto & can_remove_required_output = steps[i].can_remove_required_output;

        if (i + 1 < static_cast<int>(steps.size()))
        {
            const NameSet & additional_input = steps[i + 1].additional_input;
            for (const auto & it : steps[i + 1].actions->getRequiredColumnsWithTypes())
            {
                if (additional_input.count(it.name) == 0)
                {
                    auto iter = required_output_indexes.find(it.name);
                    if (iter == required_output_indexes.end())
                        required_output.push_back(it.name);
                    else if (!can_remove_required_output.empty())
                        can_remove_required_output[iter->second] = false;
                }
            }
        }
        steps[i].actions->finalize(required_output);
    }

    /// When possible, move the ARRAY JOIN from earlier steps to later steps.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        ExpressionAction action;
        if (steps[i - 1].actions->popUnusedArrayJoin(steps[i - 1].required_output, action))
            steps[i].actions->prependArrayJoin(action, steps[i - 1].actions->getSampleBlock());
    }

    /// Adding the ejection of unnecessary columns to the beginning of each step.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        size_t columns_from_previous = steps[i - 1].actions->getSampleBlock().columns();

        /// If unnecessary columns are formed at the output of the previous step, we'll add them to the beginning of this step.
        /// Except when we drop all the columns and lose the number of rows in the block.
        if (!steps[i].actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > steps[i].actions->getRequiredColumnsWithTypes().size())
            steps[i].actions->prependProjectInput();
    }
}

std::string ExpressionActionsChain::dumpChain() const
{
    std::stringstream ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "required output:\n";
        for (const std::string & name : steps[i].required_output)
            ss << name << "\n";
        ss << "\n" << steps[i].actions->dumpActions() << "\n";
    }

    return ss.str();
}

}
