#include <Storages/KVStorageUtils.h>

#include <unordered_map>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/Utils.h>
#include <Common/assert_cast.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Helper function to create a Tuple field from multiple field values
Field createTupleField(const std::vector<Field> & fields)
{
    Tuple tuple;
    tuple.reserve(fields.size());
    for (const auto & field : fields)
        tuple.push_back(field);
    return Field(std::move(tuple));
}

/// Helper function to compute Cartesian product of field vectors
/// Each result element is a Tuple field combining one element from each input vector
std::vector<Field> cartesianProduct(const std::vector<FieldVector> & column_values)
{
    if (column_values.empty())
        return {};

    if (column_values.size() == 1)
    {
        // Single column - wrap each value in a tuple for consistency
        std::vector<Field> result;
        for (const auto & val : column_values[0])
            result.push_back(createTupleField({val}));
        return result;
    }

    // Multi-column Cartesian product
    std::vector<Field> result;
    std::vector<size_t> indices(column_values.size(), 0);

    while (true)
    {
        // Create tuple from current indices
        std::vector<Field> tuple_values;
        tuple_values.reserve(column_values.size());
        for (size_t i = 0; i < column_values.size(); ++i)
            tuple_values.push_back(column_values[i][indices[i]]);

        result.push_back(createTupleField(tuple_values));

        // Increment indices (like odometer)
        size_t pos = column_values.size() - 1;
        while (true)
        {
            ++indices[pos];
            if (indices[pos] < column_values[pos].size())
                break;

            indices[pos] = 0;
            if (pos == 0)
                return result; // All combinations generated
            --pos;
        }
    }
}

/// Single-column version of traverseDAGFilter (legacy)
bool traverseDAGFilterSingleColumn(
    const std::string & primary_key,
    const DataTypePtr & primary_key_type,
    const ActionsDAG::Node * elem,
    const ContextPtr & context,
    FieldVectorPtr & res)
{
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilterSingleColumn(primary_key, primary_key_type, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();

    if (func_name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto * child : elem->children)
            if (traverseDAGFilterSingleColumn(primary_key, primary_key_type, child, context, res))
                return true;
        return false;
    }
    if (func_name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto * child : elem->children)
            if (!traverseDAGFilterSingleColumn(primary_key, primary_key_type, child, context, res))
                return false;
        return true;
    }
    if (func_name == "equals")
    {
        if (elem->children.size() != 2)
            return false;

        const auto * key = elem->children.at(0);
        while (key->type == ActionsDAG::ActionType::ALIAS)
            key = key->children.at(0);

        if (key->type != ActionsDAG::ActionType::INPUT)
            return false;

        if (key->result_name != primary_key)
            return false;

        const auto * value = elem->children.at(1);
        if (value->type != ActionsDAG::ActionType::COLUMN)
            return false;

        auto converted_field = convertFieldToType((*value->column)[0], *primary_key_type);
        if (!converted_field.isNull())
            res->push_back(converted_field);
        return true;
    }
    if (func_name == "in" || func_name == "globalIn")
    {
        if (elem->children.size() != 2)
            return false;

        const auto * key = elem->children.at(0);
        while (key->type == ActionsDAG::ActionType::ALIAS)
            key = key->children.at(0);

        if (key->type != ActionsDAG::ActionType::INPUT)
            return false;

        if (key->result_name != primary_key)
            return false;

        const auto * value = elem->children.at(1);
        if (value->type != ActionsDAG::ActionType::COLUMN)
            return false;

        const IColumn * value_col = value->column.get();
        if (const auto * col_const = typeid_cast<const ColumnConst *>(value_col))
            value_col = &col_const->getDataColumn();

        const auto * col_set = typeid_cast<const ColumnSet *>(value_col);
        if (!col_set)
            return false;

        auto future_set = col_set->getData();
        future_set->buildOrderedSetInplace(context);

        auto set = future_set->get();
        if (!set)
            return false;

        if (!set->hasExplicitSetElements())
            return false;

        set->checkColumnsNumber(1);
        const auto set_column_ptr = set->getSetElements()[0];
        const auto set_data_type_ptr = set->getElementsTypes()[0];
        const ColumnWithTypeAndName set_column = {set_column_ptr, set_data_type_ptr, ""};

        // In case set values can be safely casted to PK data type, simply convert them,
        // and return as filter.
        //
        // When safe cast is not guaranteed, we still can try to convert set values, and
        // push them to set filter if the whole set has been casted successfully. If some
        // of the set values haven't been casted, the set is considered as non-containing
        // keys.
        //
        // NOTE: this behavior may affect a queries like `key in (NULL)`, but right now it
        // already triggers full scan.
        if (canBeSafelyCast(set_data_type_ptr, primary_key_type))
        {
            const auto casted_set_ptr = castColumnAccurate(set_column, primary_key_type);
            const auto & casted_set = *casted_set_ptr;
            for (size_t row = 0; row < set_column_ptr->size(); ++row)
                res->push_back(casted_set[row]);
            return true;
        }
        else
        {
            const auto casted_set_ptr = castColumnAccurateOrNull(set_column, primary_key_type);
            const auto & casted_set_nullable = assert_cast<const ColumnNullable &>(*casted_set_ptr);
            const auto & casted_set_null_map = casted_set_nullable.getNullMapData();
            for (char8_t i : casted_set_null_map)
            {
                if (i != 0)
                    return false;
            }

            const auto & casted_set_inner = casted_set_nullable.getNestedColumn();
            for (size_t row = 0; row < casted_set_inner.size(); row++)
            {
                res->push_back(casted_set_inner[row]);
            }
            return true;
        }
    }
    return false;
}

/// Multi-column version of traverseDAGFilter
/// For multi-column keys, this function extracts composite key values as Tuple fields
/// Supports: tuple equality, tuple IN, and AND of individual column filters
bool traverseDAGFilter(
    const Names & primary_keys,
    const DataTypes & primary_key_types,
    const ActionsDAG::Node * elem,
    const ContextPtr & context,
    FieldVectorPtr & res)
{
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilter(primary_keys, primary_key_types, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();

    // Handle tuple equality: (key1, key2) = (val1, val2)
    if (func_name == "equals")
    {
        if (elem->children.size() != 2)
            return false;

        const auto * left = elem->children.at(0);
        const auto * right = elem->children.at(1);

        // Skip aliases
        while (left->type == ActionsDAG::ActionType::ALIAS)
            left = left->children.at(0);

        // Check if left side is tuple function with our key columns
        if (left->type == ActionsDAG::ActionType::FUNCTION && left->function_base->getName() == "tuple")
        {
            // Verify all tuple elements are our primary key columns in order
            if (left->children.size() != primary_keys.size())
                return false;

            for (size_t i = 0; i < primary_keys.size(); ++i)
            {
                const auto * key_node = left->children.at(i);
                while (key_node->type == ActionsDAG::ActionType::ALIAS)
                    key_node = key_node->children.at(0);

                if (key_node->type != ActionsDAG::ActionType::INPUT || key_node->result_name != primary_keys[i])
                    return false;
            }

            // Extract tuple value from right side
            if (right->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const auto & value_field = (*right->column)[0];
            if (value_field.getType() != Field::Types::Tuple)
                return false;

            const auto & tuple_value = value_field.safeGet<Tuple>();
            if (tuple_value.size() != primary_keys.size())
                return false;

            // Convert each tuple element to the correct type
            std::vector<Field> converted_values;
            converted_values.reserve(tuple_value.size());
            for (size_t i = 0; i < tuple_value.size(); ++i)
            {
                auto converted = convertFieldToType(tuple_value[i], *primary_key_types[i]);
                if (converted.isNull())
                    return false;
                converted_values.push_back(converted);
            }

            res->push_back(createTupleField(converted_values));
            return true;
        }

        // Fall back to single-column handling for individual column equality
        // This will be caught by AND handler below
        return false;
    }

    // Handle tuple IN: (key1, key2) IN ((v1, v2), (v3, v4))
    if (func_name == "in" || func_name == "globalIn")
    {
        if (elem->children.size() != 2)
            return false;

        const auto * left = elem->children.at(0);
        const auto * right = elem->children.at(1);

        while (left->type == ActionsDAG::ActionType::ALIAS)
            left = left->children.at(0);

        // Check if left side is tuple function with our key columns
        if (left->type == ActionsDAG::ActionType::FUNCTION && left->function_base->getName() == "tuple")
        {
            // Verify all tuple elements are our primary key columns in order
            if (left->children.size() != primary_keys.size())
                return false;

            for (size_t i = 0; i < primary_keys.size(); ++i)
            {
                const auto * key_node = left->children.at(i);
                while (key_node->type == ActionsDAG::ActionType::ALIAS)
                    key_node = key_node->children.at(0);

                if (key_node->type != ActionsDAG::ActionType::INPUT || key_node->result_name != primary_keys[i])
                    return false;
            }

            // Extract set values from right side
            if (right->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const IColumn * value_col = right->column.get();
            if (const auto * col_const = typeid_cast<const ColumnConst *>(value_col))
                value_col = &col_const->getDataColumn();

            const auto * col_set = typeid_cast<const ColumnSet *>(value_col);
            if (!col_set)
                return false;

            auto future_set = col_set->getData();
            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set || !set->hasExplicitSetElements())
                return false;

            // Set should have same number of columns as primary keys
            set->checkColumnsNumber(primary_keys.size());

            // Extract all tuple values from the set
            const auto & set_elements = set->getSetElements();

            if (set_elements.empty())
                return false;

            size_t num_rows = set_elements[0]->size();

            for (size_t row = 0; row < num_rows; ++row)
            {
                std::vector<Field> tuple_values;
                tuple_values.reserve(primary_keys.size());

                bool all_converted = true;
                for (size_t col = 0; col < primary_keys.size(); ++col)
                {
                    Field field;
                    set_elements[col]->get(row, field);
                    auto converted = convertFieldToType(field, *primary_key_types[col]);
                    if (converted.isNull())
                    {
                        all_converted = false;
                        break;
                    }
                    tuple_values.push_back(converted);
                }

                if (all_converted)
                    res->push_back(createTupleField(tuple_values));
            }

            return !res->empty();
        }

        return false;
    }

    // Handle AND: key1 = v1 AND key2 = v2
    if (func_name == "and")
    {
        // Collect filters for each key column separately
        std::unordered_map<String, FieldVector> column_filters;

        for (const auto * child : elem->children)
        {
            // Try to extract filter for each individual key column
            for (size_t i = 0; i < primary_keys.size(); ++i)
            {
                FieldVectorPtr child_res = std::make_shared<FieldVector>();
                if (traverseDAGFilterSingleColumn(primary_keys[i], primary_key_types[i], child, context, child_res))
                {
                    if (!child_res->empty())
                    {
                        for (const auto & val : *child_res)
                            column_filters[primary_keys[i]].push_back(val);
                    }
                }
            }
        }

        // Check if we have filters for ALL key columns
        if (column_filters.size() != primary_keys.size())
            return false;

        // Ensure columns are in correct order
        std::vector<FieldVector> ordered_filters;
        ordered_filters.reserve(primary_keys.size());
        for (const auto & key_name : primary_keys)
        {
            auto it = column_filters.find(key_name);
            if (it == column_filters.end() || it->second.empty())
                return false;
            ordered_filters.push_back(it->second);
        }

        // Compute Cartesian product
        auto product = cartesianProduct(ordered_filters);
        for (const auto & tuple_field : product)
            res->push_back(tuple_field);

        return !res->empty();
    }

    // Handle OR: all branches must provide complete keys
    if (func_name == "or")
    {
        for (const auto * child : elem->children)
        {
            if (!traverseDAGFilter(primary_keys, primary_key_types, child, context, res))
                return false;
        }
        return true;
    }

    return false;
}

}

std::pair<FieldVectorPtr, bool> getFilterKeys(
    const String & primary_key, const DataTypePtr & primary_key_type, const ActionsDAG * filter_actions_dag, const ContextPtr & context)
{
    if (!filter_actions_dag)
        return {{}, true};

    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseDAGFilterSingleColumn(primary_key, primary_key_type, predicate, context, res);

    return std::make_pair(res, !matched_keys);
}

std::pair<FieldVectorPtr, bool> getFilterKeys(
    const Names & primary_keys, const DataTypes & primary_key_types, const ActionsDAG * filter_actions_dag, const ContextPtr & context)
{
    if (primary_keys.size() == 1)
        return getFilterKeys(primary_keys.front(), primary_key_types.front(), filter_actions_dag, context);

    if (!filter_actions_dag)
        return {{}, true};

    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseDAGFilter(primary_keys, primary_key_types, predicate, context, res);

    return std::make_pair(res, !matched_keys);
}

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it, FieldVector::const_iterator end, DataTypePtr key_column_type, size_t max_block_size)
{
    size_t num_keys = end - it;

    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it < end && (max_block_size == 0 || rows_processed < max_block_size))
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        key_column_type->getDefaultSerialization()->serializeBinary(*it, wb, {});

        ++it;
        ++rows_processed;
    }
    return result;
}

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it, FieldVector::const_iterator end, const DataTypes & key_column_types, size_t max_block_size)
{
    if (key_column_types.empty())
        return {};

    size_t num_keys = end - it;
    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it < end && (max_block_size == 0 || rows_processed < max_block_size))
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);

        if (key_column_types.size() == 1)
        {
            // Single column: field is a plain value
            key_column_types[0]->getDefaultSerialization()->serializeBinary(*it, wb, {});
        }
        else
        {
            // Multi-column: field should be a Tuple
            if (it->getType() == Field::Types::Tuple)
            {
                const auto & tuple = it->safeGet<Tuple>();

                // Serialize each tuple element with its corresponding type
                size_t num_elements = std::min(tuple.size(), key_column_types.size());
                for (size_t i = 0; i < num_elements; ++i)
                {
                    key_column_types[i]->getDefaultSerialization()->serializeBinary(tuple[i], wb, {});
                }
            }
            else
            {
                // Unexpected format - this shouldn't happen with correct getFilterKeys usage
                // Skip this key by removing the empty string we just added
                result.pop_back();
            }
        }

        ++it;
        ++rows_processed;
    }
    return result;
}

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys)
{
    if (!keys.column)
        return {};

    size_t num_keys = keys.column->size();
    std::vector<std::string> result;
    result.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        Field field;
        keys.column->get(i, field);
        /// TODO(@vdimir): use serializeBinaryBulk
        keys.type->getDefaultSerialization()->serializeBinary(field, wb, {});
    }
    return result;
}

/// In current implementation redis / keeper can have key with only one column.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key)
{
    if (primary_key.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one primary key is supported");
    return header.getPositionByName(primary_key[0]);
}
}
