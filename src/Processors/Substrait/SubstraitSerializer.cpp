#include <Processors/Substrait/SubstraitSerializer.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Block.h>
#include <Common/Exception.h>

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
        // For now, we'll use a placeholder table name
        auto * named_table = read_rel->mutable_named_table();
        named_table->add_names("table");
    }

    void convertFilterStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter step must have a child");
        
        auto * filter_rel = rel->mutable_filter();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        filter_rel->mutable_input()->Swap(&child_rel);
        
        // TODO: Convert filter expression
        // For now, create a placeholder literal true condition
        auto * condition = filter_rel->mutable_condition();
        auto * literal = condition->mutable_literal();
        literal->set_boolean(true);
    }

    void convertExpressionStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression step must have a child");
        
        auto * project_rel = rel->mutable_project();
        
        // Convert child node
        auto child_rel = convertNode(node->children[0]);
        project_rel->mutable_input()->Swap(&child_rel);
        
        // TODO: Convert expressions to projections
        // For now, just pass through all columns
        const auto & header_ptr = node->step->getOutputHeader();
        const Block & header = *header_ptr;
        int field_index = 0;
        for (const auto & column : header)
        {
            (void)column; // Suppress unused variable warning
            auto * expr = project_rel->add_expressions();
            auto * selection = expr->mutable_selection();
            auto * direct_ref = selection->mutable_direct_reference();
            direct_ref->mutable_struct_field()->set_field(field_index++);
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
        
        // TODO: Convert grouping keys and aggregate functions
        // For now, create empty aggregation
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
