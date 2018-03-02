#include <Common/ProfileEvents.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Join.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <set>
#include <optional>


namespace ProfileEvents
{
    extern const Event FunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_ACTION;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int TOO_MUCH_TEMPORARY_COLUMNS;
    extern const int TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS;
}


Names ExpressionAction::getNeededColumns() const
{
    Names res = argument_names;

    res.insert(res.end(), array_joined_columns.begin(), array_joined_columns.end());

    for (const auto & column : projection)
        res.push_back(column.first);

    if (!source_name.empty())
        res.push_back(source_name);

    return res;
}


ExpressionAction ExpressionAction::applyFunction(const FunctionBuilderPtr & function_,
    const std::vector<std::string> & argument_names_,
    std::string result_name_)
{
    if (result_name_ == "")
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

ExpressionAction ExpressionAction::addColumn(const ColumnWithTypeAndName & added_column_)
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

ExpressionAction ExpressionAction::copyColumn(const std::string & from_name, const std::string & to_name)
{
    ExpressionAction a;
    a.type = COPY_COLUMN;
    a.source_name = from_name;
    a.result_name = to_name;
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

ExpressionAction ExpressionAction::arrayJoin(const NameSet & array_joined_columns, bool array_join_is_left, const Context & context)
{
    if (array_joined_columns.empty())
        throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);
    ExpressionAction a;
    a.type = ARRAY_JOIN;
    a.array_joined_columns = array_joined_columns;
    a.array_join_is_left = array_join_is_left;

    if (array_join_is_left)
        a.function_builder = FunctionFactory::instance().get("emptyArrayToSingle", context);

    return a;
}

ExpressionAction ExpressionAction::ordinaryJoin(std::shared_ptr<const Join> join_, const NamesAndTypesList & columns_added_by_join_)
{
    ExpressionAction a;
    a.type = JOIN;
    a.join = join_;
    a.columns_added_by_join = columns_added_by_join_;
    return a;
}


void ExpressionAction::prepare(Block & sample_block)
{
//    std::cerr << "preparing: " << toString() << std::endl;

    /** Constant expressions should be evaluated, and put the result in sample_block.
      */

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            if (sample_block.has(result_name))
                throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

            bool all_const = true;

            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                arguments[i] = sample_block.getPositionByName(argument_names[i]);
                ColumnPtr col = sample_block.safeGetByPosition(arguments[i]).column;
                if (!col || !col->isColumnConst())
                    all_const = false;
            }

            ColumnPtr new_column;

            /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
            if (all_const && function->isSuitableForConstantFolding())
            {
                size_t result_position = sample_block.columns();

                ColumnWithTypeAndName new_column;
                new_column.name = result_name;
                new_column.type = result_type;
                sample_block.insert(std::move(new_column));

                function->execute(sample_block, arguments, result_position);

                /// If the result is not a constant, just in case, we will consider the result as unknown.
                ColumnWithTypeAndName & col = sample_block.safeGetByPosition(result_position);
                if (!col.column->isColumnConst())
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
                }
            }
            else
            {
                sample_block.insert({nullptr, result_type, result_name});
            }

            break;
        }

        case ARRAY_JOIN:
        {
            for (const auto & name : array_joined_columns)
            {
                ColumnWithTypeAndName & current = sample_block.getByName(name);
                const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*current.type);
                if (!array_type)
                    throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);
                current.type = array_type->getNestedType();
                current.column = nullptr;
            }

            break;
        }

        case JOIN:
        {
            /// TODO join_use_nulls setting

            for (const auto & col : columns_added_by_join)
                sample_block.insert(ColumnWithTypeAndName(nullptr, col.type, col.name));

            break;
        }

        case PROJECT:
        {
            Block new_block;

            for (size_t i = 0; i < projection.size(); ++i)
            {
                const std::string & name = projection[i].first;
                const std::string & alias = projection[i].second;
                ColumnWithTypeAndName column = sample_block.getByName(name);
                if (alias != "")
                    column.name = alias;
                new_block.insert(std::move(column));
            }

            sample_block.swap(new_block);
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
            result_type = sample_block.getByName(source_name).type;
            sample_block.insert(ColumnWithTypeAndName(sample_block.getByName(source_name).column, result_type, result_name));
            break;
        }

        default:
            throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
    }
}


void ExpressionAction::execute(Block & block) const
{
//    std::cerr << "executing: " << toString() << std::endl;

    if (type == REMOVE_COLUMN || type == COPY_COLUMN)
        if (!block.has(source_name))
            throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    if (type == ADD_COLUMN || type == COPY_COLUMN || type == APPLY_FUNCTION)
        if (block.has(result_name))
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                if (!block.has(argument_names[i]))
                    throw Exception("Not found column: '" + argument_names[i] + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                arguments[i] = block.getPositionByName(argument_names[i]);
            }

            size_t num_columns_without_result = block.columns();
            block.insert({ nullptr, result_type, result_name});

            ProfileEvents::increment(ProfileEvents::FunctionExecute);
            function->execute(block, arguments, num_columns_without_result);

            break;
        }

        case ARRAY_JOIN:
        {
            if (array_joined_columns.empty())
                throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);

            ColumnPtr any_array_ptr = block.getByName(*array_joined_columns.begin()).column;
            if (ColumnPtr converted = any_array_ptr->convertToFullColumnIfConst())
                any_array_ptr = converted;

            const ColumnArray * any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
            if (!any_array)
                throw Exception("ARRAY JOIN of not array: " + *array_joined_columns.begin(), ErrorCodes::TYPE_MISMATCH);

            /// If LEFT ARRAY JOIN, then we create columns in which empty arrays are replaced by arrays with one element - the default value.
            std::map<String, ColumnPtr> non_empty_array_columns;
            if (array_join_is_left)
            {
                for (const auto & name : array_joined_columns)
                {
                    auto src_col = block.getByName(name);

                    Block tmp_block{src_col, {{}, src_col.type, {}}};

                    function_builder->build({src_col})->execute(tmp_block, {0}, 1);
                    non_empty_array_columns[name] = tmp_block.safeGetByPosition(1).column;
                }

                any_array_ptr = non_empty_array_columns.begin()->second;
                if (ColumnPtr converted = any_array_ptr->convertToFullColumnIfConst())
                    any_array_ptr = converted;

                any_array = &typeid_cast<const ColumnArray &>(*any_array_ptr);
            }

            size_t columns = block.columns();
            for (size_t i = 0; i < columns; ++i)
            {
                ColumnWithTypeAndName & current = block.safeGetByPosition(i);

                if (array_joined_columns.count(current.name))
                {
                    if (!typeid_cast<const DataTypeArray *>(&*current.type))
                        throw Exception("ARRAY JOIN of not array: " + current.name, ErrorCodes::TYPE_MISMATCH);

                    ColumnPtr array_ptr = array_join_is_left ? non_empty_array_columns[current.name] : current.column;

                    if (ColumnPtr converted = array_ptr->convertToFullColumnIfConst())
                        array_ptr = converted;

                    const ColumnArray & array = typeid_cast<const ColumnArray &>(*array_ptr);
                    if (!array.hasEqualOffsets(typeid_cast<const ColumnArray &>(*any_array_ptr)))
                        throw Exception("Sizes of ARRAY-JOIN-ed arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

                    current.column = typeid_cast<const ColumnArray &>(*array_ptr).getDataPtr();
                    current.type = typeid_cast<const DataTypeArray &>(*current.type).getNestedType();
                }
                else
                {
                    current.column = current.column->replicate(any_array->getOffsets());
                }
            }

            break;
        }

        case JOIN:
        {
            join->joinBlock(block);
            break;
        }

        case PROJECT:
        {
            Block new_block;

            for (size_t i = 0; i < projection.size(); ++i)
            {
                const std::string & name = projection[i].first;
                const std::string & alias = projection[i].second;
                ColumnWithTypeAndName column = block.getByName(name);
                if (alias != "")
                    column.name = alias;
                new_block.insert(std::move(column));
            }

            block.swap(new_block);

            break;
        }

        case REMOVE_COLUMN:
            block.erase(source_name);
            break;

        case ADD_COLUMN:
            block.insert({ added_column->cloneResized(block.rows()), result_type, result_name });
            break;

        case COPY_COLUMN:
            block.insert({ block.getByName(source_name).column, result_type, result_name });
            break;

        default:
            throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
    }
}


void ExpressionAction::executeOnTotals(Block & block) const
{
    if (type != JOIN)
        execute(block);
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
            break;

        case APPLY_FUNCTION:
            ss << "FUNCTION " << result_name << " "
                << (result_type ? result_type->getName() : "(no type)") << " = "
                << (function ? function->getName() : "(no function)") << "(";
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << argument_names[i];
            }
            ss << ")";
            break;

        case ARRAY_JOIN:
            ss << (array_join_is_left ? "LEFT " : "") << "ARRAY JOIN ";
            for (NameSet::const_iterator it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it)
            {
                if (it != array_joined_columns.begin())
                    ss << ", ";
                ss << *it;
            }
            break;

        case JOIN:
            ss << "JOIN ";
            for (NamesAndTypesList::const_iterator it = columns_added_by_join.begin(); it != columns_added_by_join.end(); ++it)
            {
                if (it != columns_added_by_join.begin())
                    ss << ", ";
                ss << it->name;
            }
            break;

        case PROJECT:
            ss << "PROJECT ";
            for (size_t i = 0; i < projection.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << projection[i].first;
                if (projection[i].second != "" && projection[i].second != projection[i].first)
                    ss << " AS " << projection[i].second;
            }
            break;

        default:
            throw Exception("Unexpected Action type", ErrorCodes::LOGICAL_ERROR);
    }

    return ss.str();
}

void ExpressionActions::checkLimits(Block & block) const
{
    const Limits & limits = settings.limits;
    if (limits.max_temporary_columns && block.columns() > limits.max_temporary_columns)
        throw Exception("Too many temporary columns: " + block.dumpNames()
            + ". Maximum: " + limits.max_temporary_columns.toString(),
            ErrorCodes::TOO_MUCH_TEMPORARY_COLUMNS);

    if (limits.max_temporary_non_const_columns)
    {
        size_t non_const_columns = 0;
        for (size_t i = 0, size = block.columns(); i < size; ++i)
            if (block.safeGetByPosition(i).column && !block.safeGetByPosition(i).column->isColumnConst())
                ++non_const_columns;

        if (non_const_columns > limits.max_temporary_non_const_columns)
        {
            std::stringstream list_of_non_const_columns;
            for (size_t i = 0, size = block.columns(); i < size; ++i)
                if (!block.safeGetByPosition(i).column->isColumnConst())
                    list_of_non_const_columns << "\n" << block.safeGetByPosition(i).name;

            throw Exception("Too many temporary non-const columns:" + list_of_non_const_columns.str()
                + ". Maximum: " + limits.max_temporary_non_const_columns.toString(),
                ErrorCodes::TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS);
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
    if (sample_block.has(action.result_name))
        return;

    if (action.result_name != "")
        new_names.push_back(action.result_name);
    new_names.insert(new_names.end(), action.array_joined_columns.begin(), action.array_joined_columns.end());

    if (action.type == ExpressionAction::APPLY_FUNCTION)
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

        action.function = action.function_builder->build(arguments);
        action.result_type = action.function->getReturnType();
    }

    action.prepare(sample_block);
    actions.push_back(action);
}

void ExpressionActions::prependProjectInput()
{
    actions.insert(actions.begin(), ExpressionAction::project(getRequiredColumns()));
}

void ExpressionActions::prependArrayJoin(const ExpressionAction & action, const Block & sample_block)
{
    if (action.type != ExpressionAction::ARRAY_JOIN)
        throw Exception("ARRAY_JOIN action expected", ErrorCodes::LOGICAL_ERROR);

    NameSet array_join_set(action.array_joined_columns.begin(), action.array_joined_columns.end());
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
        input_columns.emplace_back(name, sample_block.getByName(name).type);
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
    for (const std::string & name : actions.back().array_joined_columns)
    {
        if (required_set.count(name))
            return false;
    }
    for (const std::string & name : actions.back().array_joined_columns)
    {
        DataTypePtr & type = sample_block.getByName(name).type;
        type = std::make_shared<DataTypeArray>(type);
    }
    out_action = actions.back();
    actions.pop_back();
    return true;
}

void ExpressionActions::execute(Block & block) const
{
    for (const auto & action : actions)
    {
        action.execute(block);
        checkLimits(block);
    }
}

void ExpressionActions::executeOnTotals(Block & block) const
{
    /// If there is `totals` in the subquery for JOIN, but we do not have totals, then take the block with the default values instead of `totals`.
    if (!block)
    {
        bool has_totals_in_join = false;
        for (const auto & action : actions)
        {
            if (action.join && action.join->hasTotals())
            {
                has_totals_in_join = true;
                break;
            }
        }

        if (has_totals_in_join)
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
    for (size_t i = 0; i < output_columns.size(); ++i)
    {
        const std::string & name = output_columns[i];
        if (!sample_block.has(name))
            throw Exception("Unknown column: " + name + ", there are only columns "
                            + sample_block.dumpNames(), ErrorCodes::UNKNOWN_IDENTIFIER);
        final_columns.insert(name);
    }

    /// Which columns are needed to perform actions from the current to the last.
    NameSet needed_columns = final_columns;
    /// Which columns nobody will touch from the current action to the last.
    NameSet unmodified_columns;

    {
        NamesAndTypesList sample_columns = sample_block.getNamesAndTypesList();
        for (NamesAndTypesList::iterator it = sample_columns.begin(); it != sample_columns.end(); ++it)
            unmodified_columns.insert(it->name);
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
        else if (action.type == ExpressionAction::ARRAY_JOIN)
        {
            /// Do not ARRAY JOIN columns that are not used anymore.
            /// Usually, such columns are not used until ARRAY JOIN, and therefore are ejected further in this function.
            /// We will not remove all the columns so as not to lose the number of rows.
            for (auto it = action.array_joined_columns.begin(); it != action.array_joined_columns.end();)
            {
                bool need = needed_columns.count(*it);
                if (!need && action.array_joined_columns.size() > 1)
                {
                    action.array_joined_columns.erase(it++);
                }
                else
                {
                    needed_columns.insert(*it);
                    unmodified_columns.erase(*it);

                    /// If no ARRAY JOIN results are used, forcibly leave an arbitrary column at the output,
                    ///  so you do not lose the number of rows.
                    if (!need)
                        final_columns.insert(*it);

                    ++it;
                }
            }
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
                    if (result.column)
                    {
                        action.type = ExpressionAction::ADD_COLUMN;
                        action.result_type = result.type;
                        action.added_column = result.column;
                        action.function_builder = nullptr;
                        action.function = nullptr;
                        action.argument_names.clear();
                        in.clear();
                    }
                }
            }

            needed_columns.insert(in.begin(), in.end());
        }
    }

    /// We will not throw out all the input columns, so as not to lose the number of rows in the block.
    if (needed_columns.empty() && !input_columns.empty())
        needed_columns.insert(getSmallestColumn(input_columns));

    /// We will not leave the block empty so as not to lose the number of rows in it.
    if (final_columns.empty())
        final_columns.insert(getSmallestColumn(input_columns));

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

    optimize();
    checkLimits(sample_block);
}


std::string ExpressionActions::dumpActions() const
{
    std::stringstream ss;

    ss << "input:\n";
    for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
        ss << it->name << " " << it->type->getName() << "\n";

    ss << "\nactions:\n";
    for (size_t i = 0; i < actions.size(); ++i)
        ss << actions[i].toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (NamesAndTypesList::const_iterator it = output_columns.begin(); it != output_columns.end(); ++it)
        ss << it->name << " " << it->type->getName() << "\n";

    return ss.str();
}

void ExpressionActions::optimize()
{
    optimizeArrayJoin();
}

void ExpressionActions::optimizeArrayJoin()
{
    const size_t NONE = actions.size();
    size_t first_array_join = NONE;

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
            if (first_array_join == NONE)
                continue;

            needed = actions[i].getNeededColumns();

            for (size_t j = 0; j < needed.size(); ++j)
            {
                if (array_joined_columns.count(needed[j]))
                {
                    depends_on_array_join = true;
                    break;
                }
            }
        }

        if (depends_on_array_join)
        {
            if (first_array_join == NONE)
                first_array_join = i;

            if (actions[i].result_name != "")
                array_joined_columns.insert(actions[i].result_name);
            array_joined_columns.insert(actions[i].array_joined_columns.begin(), actions[i].array_joined_columns.end());

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


BlockInputStreamPtr ExpressionActions::createStreamWithNonJoinedDataIfFullOrRightJoin(const Block & source_header, size_t max_block_size) const
{
    for (const auto & action : actions)
        if (action.join && (action.join->getKind() == ASTTableJoin::Kind::Full || action.join->getKind() == ASTTableJoin::Kind::Right))
            return action.join->createStreamWithNonJoinedRows(source_header, max_block_size);

    return {};
}


void ExpressionActionsChain::addStep()
{
    if (steps.empty())
        throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumnsWithTypeAndName();
    steps.push_back(Step(std::make_shared<ExpressionActions>(columns, settings)));
}

void ExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        Names required_output = steps[i].required_output;
        if (i + 1 < static_cast<int>(steps.size()))
        {
            for (const auto & it : steps[i + 1].actions->getRequiredColumnsWithTypes())
                required_output.push_back(it.name);
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

std::string ExpressionActionsChain::dumpChain()
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
