#include <Processors/Substrait/SubstraitSerializer.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Common/Exception.h>
#include <Storages/IStorage.h>

// Include generated Substrait protobuf headers
#include <substrait/plan.pb.h>
#include <substrait/algebra.pb.h>
#include <substrait/type.pb.h>
#include <substrait/extensions/extensions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class SubstraitSerializer::Impl
{
public:
    explicit Impl() = default;

    void convertPlan(const QueryPlan & query_plan, substrait::Plan & substrait_plan)
    {
        if (!query_plan.isInitialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize uninitialized query plan to Substrait");

        // Set Substrait version
        auto * version = substrait_plan.mutable_version();
        version->set_major_number(0);
        version->set_minor_number(54);  // Latest Substrait version as of implementation
        version->set_patch_number(0);
        version->set_producer("ClickHouse");

        // Get the root node of the QueryPlan
        auto * root = query_plan.getRootNode();
        if (!root)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan has no root node");
        
        // Convert QueryPlan tree to Substrait
        auto * plan_rel = substrait_plan.add_relations();
        auto * root_rel = plan_rel->mutable_root();
        auto rel = convertNode(root);
        root_rel->mutable_input()->Swap(&rel);
    }

private:
    /// Convert ClickHouse DataType to Substrait Type
    void convertType(const DataTypePtr & ch_type, substrait::Type * substrait_type)
    {
        TypeIndex type_id = ch_type->getTypeId();
        
        switch (type_id)
        {
            case TypeIndex::Int32:
                substrait_type->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::Int64:
                substrait_type->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::UInt32:
                // Substrait doesn't have unsigned types, map to signed with larger width
                substrait_type->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::UInt64:
                // Map UInt64 to Int64 (note: may lose precision for large values)
                substrait_type->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::Float32:
                substrait_type->mutable_fp32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::Float64:
                substrait_type->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::String:
                substrait_type->mutable_string()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            case TypeIndex::UInt8:
                // Bool in ClickHouse is represented as UInt8
                if (ch_type->getName() == "Bool")
                    substrait_type->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                else
                    substrait_type->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
                break;
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, 
                    "Type {} not yet supported for Substrait conversion", ch_type->getName());
        }
    }

    /// Convert ActionsDAG Node to Substrait Expression
    substrait::Expression convertExpression(const ActionsDAG::Node * node, const Block & input_header)
    {
        substrait::Expression expr;
        
        if (!node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert null ActionsDAG node");
        
        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                // Column reference - find column index in input
                auto * selection = expr.mutable_selection();
                auto * direct_ref = selection->mutable_direct_reference();
                auto * struct_field = direct_ref->mutable_struct_field();
                
                // Find column index by name
                int field_index = -1;
                for (size_t i = 0; i < input_header.columns(); ++i)
                {
                    if (input_header.getByPosition(i).name == node->result_name)
                    {
                        field_index = static_cast<int>(i);
                        break;
                    }
                }
                
                if (field_index < 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, 
                        "Column {} not found in input header", node->result_name);
                
                struct_field->set_field(field_index);
                break;
            }
            
            case ActionsDAG::ActionType::COLUMN:
            {
                // Constant value
                auto * literal = expr.mutable_literal();
                
                if (!node->column || node->column->empty())
                {
                    // NULL value - just create empty literal
                    literal->set_boolean(false); // Placeholder for NULL
                }
                else
                {
                    // Get the first value from the column
                    TypeIndex type_id = node->result_type->getTypeId();
                    auto field = (*node->column)[0];
                    
                    switch (type_id)
                    {
                        case TypeIndex::UInt8:
                            if (node->result_type->getName() == "Bool")
                                literal->set_boolean(field.safeGet<UInt8>() != 0);
                            else
                                literal->set_i32(field.safeGet<UInt8>());
                            break;
                        case TypeIndex::Int32:
                            literal->set_i32(field.safeGet<Int32>());
                            break;
                        case TypeIndex::Int64:
                            literal->set_i64(field.safeGet<Int64>());
                            break;
                        case TypeIndex::UInt32:
                            literal->set_i64(field.safeGet<UInt32>());
                            break;
                        case TypeIndex::UInt64:
                            literal->set_i64(static_cast<Int64>(field.safeGet<UInt64>()));
                            break;
                        case TypeIndex::Float32:
                            literal->set_fp32(static_cast<float>(field.safeGet<Float64>()));
                            break;
                        case TypeIndex::Float64:
                            literal->set_fp64(field.safeGet<Float64>());
                            break;
                        case TypeIndex::String:
                            literal->set_string(field.safeGet<String>());
                            break;
                        default:
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Constant type {} not yet supported", node->result_type->getName());
                    }
                }
                break;
            }
            
            case ActionsDAG::ActionType::ALIAS:
            {
                // Just pass through to the child
                if (node->children.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "ALIAS node must have a child");
                return convertExpression(node->children[0], input_header);
            }
            
            case ActionsDAG::ActionType::FUNCTION:
            {
                // Function call
                const String & func_name = node->function_base->getName();
                
                // Map ClickHouse function names to Substrait
                auto * scalar_func = expr.mutable_scalar_function();
                
                // Map function names to Substrait function references
                // In a full implementation, these would be registered in an extension URI
                static const std::unordered_map<String, int> function_map = {
                    {"equals", 1},
                    {"notEquals", 2},
                    {"less", 3},
                    {"lessOrEquals", 4},
                    {"greater", 5},
                    {"greaterOrEquals", 6},
                    {"plus", 7},
                    {"minus", 8},
                    {"multiply", 9},
                    {"divide", 10},
                    {"and", 11},
                    {"or", 12},
                    {"not", 13},
                    {"sum", 20},
                    {"avg", 21},
                    {"count", 22},
                    {"min", 23},
                    {"max", 24}
                };
                
                auto it = function_map.find(func_name);
                if (it == function_map.end())
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Function {} not yet supported for Substrait conversion", func_name);
                }
                
                // Set function reference
                scalar_func->set_function_reference(it->second);
                
                // Convert children arguments
                for (const auto * child : node->children)
                {
                    auto * arg = scalar_func->add_arguments();
                    arg->mutable_value()->CopyFrom(convertExpression(child, input_header));
                }
                
                break;
            }
            
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "ActionsDAG node type {} not yet supported for Substrait conversion",
                    static_cast<int>(node->type));
        }
        
        return expr;
    }

    /// Convert QueryPlan node to Substrait Rel
    substrait::Rel convertNode(const QueryPlan::Node * node)
    {
        if (!node || !node->step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid QueryPlan node");
        
        const auto & step = *node->step;
        const String & step_name = step.getName();
        
        substrait::Rel rel;
        
        // Handle different step types
        if (step_name == "ReadFromMergeTree" || step_name.starts_with("ReadFrom"))
        {
            convertReadStep(step, &rel);
        }
        else if (step_name == "Filter")
        {
            convertFilterStep(node, &rel);
        }
        else if (step_name == "Expression")
        {
            convertExpressionStep(node, &rel);
        }
        else if (step_name == "Sorting")
        {
            convertSortingStep(node, &rel);
        }
        else if (step_name == "Aggregating")
        {
            convertAggregatingStep(node, &rel);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, 
                "Step type '{}' not yet supported for Substrait conversion", step_name);
        }
        
        return rel;
    }

    void convertReadStep(const IQueryPlanStep & step, substrait::Rel * rel)
    {
        auto * read_rel = rel->mutable_read();
        
        // Get output header to determine schema
        const auto & header_ptr = step.getOutputHeader();
        const Block & header = *header_ptr;
        auto * base_schema = read_rel->mutable_base_schema();
        
        // Convert columns to Substrait schema
        for (const auto & column : header)
        {
            base_schema->add_names(column.name);
            auto * type = base_schema->mutable_struct_()->add_types();
            convertType(column.type, type);
        }
        
        // Create a named table reference
        auto * named_table = read_rel->mutable_named_table();
        
        // Try to extract table name from ReadFromMergeTree step
        if (step.getName() == "ReadFromMergeTree")
        {
            // Use static_cast since we've already verified the type by name
            const auto * read_from_mt = static_cast<const ReadFromMergeTree *>(&step);
            const auto & storage_id = read_from_mt->getStorageID();
            if (!storage_id.database_name.empty())
                named_table->add_names(storage_id.database_name);
            if (!storage_id.table_name.empty())
                named_table->add_names(storage_id.table_name);
            else
                named_table->add_names("table");  // Fallback if table_name is empty
        }
        else
        {
            // Fallback for other read steps
            named_table->add_names("table");
        }
    }

    void convertFilterStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter step must have a child");
        
        auto * filter_rel = rel->mutable_filter();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        filter_rel->mutable_input()->Swap(&child_rel);
        
        // Cast to FilterStep to access the ActionsDAG
        const auto * filter_step = dynamic_cast<const FilterStep *>(node->step.get());
        if (!filter_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected FilterStep but got {}", node->step->getName());
        
        // Get the filter expression from ActionsDAG
        const auto & actions_dag = filter_step->getExpression();
        const String & filter_column_name = filter_step->getFilterColumnName();
        
        // Find the filter column in the DAG outputs
        const ActionsDAG::Node * filter_node = actions_dag.tryFindInOutputs(filter_column_name);
        if (!filter_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter column {} not found in ActionsDAG outputs", filter_column_name);
        
        // Get input header from child node
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;
        
        // Convert the filter expression
        auto filter_expr = convertExpression(filter_node, input_header);
        filter_rel->mutable_condition()->Swap(&filter_expr);
    }

    void convertExpressionStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression step must have a child");
        
        auto * project_rel = rel->mutable_project();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        project_rel->mutable_input()->Swap(&child_rel);
        
        // Cast to ExpressionStep to access the ActionsDAG
        const auto * expr_step = dynamic_cast<const ExpressionStep *>(node->step.get());
        if (!expr_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ExpressionStep but got {}", node->step->getName());
        
        // Get the ActionsDAG containing the expressions
        const auto & actions_dag = expr_step->getExpression();
        
        // Get input header from child node
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;
        
        // Convert each output column expression
        const auto & outputs = actions_dag.getOutputs();
        for (const auto * output_node : outputs)
        {
            auto expr = convertExpression(output_node, input_header);
            project_rel->add_expressions()->Swap(&expr);
        }
    }   

    void convertSortingStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting step must have a child");
        
        auto * sort_rel = rel->mutable_sort();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        sort_rel->mutable_input()->Swap(&child_rel);
        
        // TODO: Convert sort description
        // For now, create a simple sort by first column
        auto * sort_field = sort_rel->add_sorts();
        auto * expr = sort_field->mutable_expr();
        auto * selection = expr->mutable_selection();
        auto * direct_ref = selection->mutable_direct_reference();
        direct_ref->mutable_struct_field()->set_field(0);
        sort_field->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);
    }

    void convertAggregatingStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregating step must have a child");
        
        auto * agg_rel = rel->mutable_aggregate();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        agg_rel->mutable_input()->Swap(&child_rel);
        
        // Cast to AggregatingStep to access aggregation parameters
        const auto * agg_step = dynamic_cast<const AggregatingStep *>(node->step.get());
        if (!agg_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected AggregatingStep but got {}", node->step->getName());
        
        const auto & params = agg_step->getParams();
        
        // Get input header from child node
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;
        
        // Convert grouping keys
        for (const auto & key : params.keys)
        {
            auto * grouping_expr = agg_rel->add_groupings()->add_grouping_expressions();
            
            // Find the column index for the grouping key
            int field_index = -1;
            for (size_t i = 0; i < input_header.columns(); ++i)
            {
                if (input_header.getByPosition(i).name == key)
                {
                    field_index = static_cast<int>(i);
                    break;
                }
            }
            
            if (field_index < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Grouping key {} not found in input", key);
            
            auto * selection = grouping_expr->mutable_selection();
            auto * direct_ref = selection->mutable_direct_reference();
            direct_ref->mutable_struct_field()->set_field(field_index);
        }
        
        // Convert aggregate functions
        for (const auto & aggregate : params.aggregates)
        {
            auto * measure = agg_rel->add_measures();
            auto * agg_func = measure->mutable_measure();
            
            // Map aggregate function name
            static const std::unordered_map<String, int> agg_function_map = {
                {"sum", 20},
                {"avg", 21},
                {"count", 22},
                {"min", 23},
                {"max", 24}
            };
            
            auto it = agg_function_map.find(aggregate.function->getName());
            if (it == agg_function_map.end())
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Aggregate function {} not yet supported for Substrait conversion",
                    aggregate.function->getName());
            }
            
            agg_func->set_function_reference(it->second);
            
            // Convert aggregate arguments
            for (const auto & arg_column : aggregate.argument_names)
            {
                // Find the column index
                int field_index = -1;
                for (size_t i = 0; i < input_header.columns(); ++i)
                {
                    if (input_header.getByPosition(i).name == arg_column)
                    {
                        field_index = static_cast<int>(i);
                        break;
                    }
                }
                
                if (field_index < 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, 
                        "Aggregate argument {} not found in input", arg_column);
                
                auto * arg = agg_func->add_arguments();
                auto * arg_expr = arg->mutable_value();
                auto * selection = arg_expr->mutable_selection();
                auto * direct_ref = selection->mutable_direct_reference();
                direct_ref->mutable_struct_field()->set_field(field_index);
            }
        }
    }

public:

    std::string serializeToBinary(const substrait::Plan & plan)
    {
        std::string output;
        if (!plan.SerializeToString(&output))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize Substrait plan to binary");
        return output;
    }

    std::string serializeToJSON(const substrait::Plan & plan)
    {
        std::string output;
        google::protobuf::json::PrintOptions json_options;
        json_options.add_whitespace = true;
        
        auto status = google::protobuf::json::MessageToJsonString(plan, &output, json_options);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to serialize Substrait plan to JSON: {}", 
                std::string(status.message()));
        
        return output;
    }
    
    std::string convertPlanToBinary(const QueryPlan & query_plan)
    {
        substrait::Plan substrait_plan;
        convertPlan(query_plan, substrait_plan);
        return serializeToBinary(substrait_plan);
    }
    
    std::string convertPlanToJSON(const QueryPlan & query_plan)
    {
        substrait::Plan substrait_plan;
        convertPlan(query_plan, substrait_plan);
        return serializeToJSON(substrait_plan);
    }
};

std::string SubstraitSerializer::serializePlanToBinary(const QueryPlan & query_plan)
{
    Impl impl;
    return impl.convertPlanToBinary(query_plan);
}

std::string SubstraitSerializer::serializePlanToJSON(const QueryPlan & query_plan)
{
    Impl impl;
    return impl.convertPlanToJSON(query_plan);
}

}
