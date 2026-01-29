#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Substrait/SubstraitSerializer.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <cmath>

#include <substrait/algebra.pb.h>
#include <substrait/extensions/extensions.pb.h>
#include <substrait/plan.pb.h>
#include <substrait/type.pb.h>

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
private:
    /// Convert a Field value to a Substrait Literal
    void convertFieldToLiteral(const Field & field, const DataTypePtr & type, substrait::Expression::Literal * literal)
    {
        if (field.isNull())
        {
            auto * null_type = literal->mutable_null();
            convertType(type, null_type);
            return;
        }

        TypeIndex type_id = type->getTypeId();

        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            convertFieldToLiteral(field, nullable->getNestedType(), literal);
            return;
        }

        if (const auto * lc = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            convertFieldToLiteral(field, lc->getDictionaryType(), literal);
            return;
        }

        switch (type_id)
        {
            case TypeIndex::Nothing: {
                // Nothing type is always null - emit typed null with nullable i32
                auto * null_type = literal->mutable_null();
                null_type->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
                return;
            }
            case TypeIndex::Int8:
                literal->set_i8(field.safeGet<Int8>());
                break;
            case TypeIndex::Int16:
                literal->set_i16(field.safeGet<Int16>());
                break;
            case TypeIndex::Int32:
                literal->set_i32(field.safeGet<Int32>());
                break;
            case TypeIndex::Int64:
                literal->set_i64(field.safeGet<Int64>());
                break;
            case TypeIndex::UInt8:
                if (type->getName() == "Bool")
                    literal->set_boolean(field.safeGet<UInt8>() != 0);
                else
                    literal->set_i16(field.safeGet<UInt8>());
                break;
            case TypeIndex::UInt16:
                literal->set_i32(field.safeGet<UInt16>());
                break;
            case TypeIndex::UInt32:
                literal->set_i64(field.safeGet<UInt32>());
                break;
            case TypeIndex::UInt64: {
                UInt64 u64 = field.safeGet<UInt64>();
                if (u64 <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                {
                    literal->set_i64(static_cast<Int64>(u64));
                }
                else
                {
                    // Values exceeding Int64 range are encoded as Decimal(20,0) to preserve precision
                    auto * decimal_literal = literal->mutable_decimal();
                    decimal_literal->set_precision(20);
                    decimal_literal->set_scale(0);
                    Int128 raw_value = static_cast<Int128>(u64);
                    std::string bytes(16, '\0');
                    unsigned __int128 u = static_cast<unsigned __int128>(raw_value);
                    // Encode as big-endian 16-byte representation
                    for (int i = 0; i < 16; ++i)
                        bytes[15 - i] = static_cast<char>((u >> (i * 8)) & 0xFF);
                    decimal_literal->set_value(std::move(bytes));
                }
                break;
            }
            case TypeIndex::Float32:
                literal->set_fp32(static_cast<float>(field.safeGet<Float64>()));
                break;
            case TypeIndex::Float64:
                literal->set_fp64(field.safeGet<Float64>());
                break;
            case TypeIndex::String:
                literal->set_string(field.safeGet<String>());
                break;
            case TypeIndex::Date:
                literal->set_date(static_cast<Int32>(field.safeGet<UInt16>()));
                break;
            case TypeIndex::DateTime:
                literal->set_timestamp(static_cast<Int64>(field.safeGet<UInt32>()) * 1000000LL);
                break;
            case TypeIndex::DateTime64: {
                const auto * dt64 = checkAndGetDataType<DataTypeDateTime64>(type.get());
                if (!dt64)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected DateTime64 type");
                Int64 raw = applyVisitor(FieldVisitorConvertToNumber<Int64>(), field);
                int scale = dt64->getScale();
                // Substrait timestamp uses microsecond precision (scale=6)
                // Adjust from ClickHouse's arbitrary scale to microseconds
                if (scale < 6)
                {
                    int diff = 6 - scale;
                    Int64 factor = 1;
                    for (int i = 0; i < diff; ++i)
                        factor *= 10;
                    raw *= factor;
                }
                else if (scale > 6)
                {
                    // Truncate sub-microsecond precision
                    int diff = scale - 6;
                    Int64 factor = 1;
                    for (int i = 0; i < diff; ++i)
                        factor *= 10;
                    raw /= factor;
                }
                literal->set_timestamp(raw);
                break;
            }
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128: {
                auto * decimal_literal = literal->mutable_decimal();
                UInt32 target_scale = getDecimalScale(*type);
                UInt32 target_precision = getDecimalPrecision(*type);

                // Extract the raw integer value from the decimal field based on its width
                Int128 raw_value = 0;
                if (type_id == TypeIndex::Decimal32)
                    raw_value = static_cast<Int128>(field.safeGet<DecimalField<Decimal32>>().getValue().value);
                else if (type_id == TypeIndex::Decimal64)
                    raw_value = static_cast<Int128>(field.safeGet<DecimalField<Decimal64>>().getValue().value);
                else
                    raw_value = field.safeGet<DecimalField<Decimal128>>().getValue().value;

                // Substrait decimals are encoded as big-endian 16-byte two's complement
                std::string bytes(16, '\0');
                unsigned __int128 u = static_cast<unsigned __int128>(raw_value);
                for (int i = 0; i < 16; ++i)
                    bytes[15 - i] = static_cast<char>((u >> (i * 8)) & 0xFF);

                decimal_literal->set_value(std::move(bytes));
                decimal_literal->set_precision(target_precision);
                decimal_literal->set_scale(target_scale);
                break;
            }
            case TypeIndex::Array: {
                const auto & arr = field.safeGet<Array>();
                auto * list_literal = literal->mutable_list();
                const auto * array_type = checkAndGetDataType<DataTypeArray>(type.get());
                if (!array_type)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Array type");

                for (const auto & elem : arr)
                {
                    auto * elem_literal = list_literal->add_values();
                    convertFieldToLiteral(elem, array_type->getNestedType(), elem_literal);
                }
                break;
            }
            case TypeIndex::Tuple: {
                const auto & tuple = field.safeGet<Tuple>();
                auto * struct_literal = literal->mutable_struct_();
                const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(type.get());
                if (!tuple_type)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Tuple type");

                const auto & element_types = tuple_type->getElements();
                for (size_t i = 0; i < tuple.size(); ++i)
                {
                    auto * elem_literal = struct_literal->add_fields();
                    convertFieldToLiteral(tuple[i], element_types[i], elem_literal);
                }
                break;
            }
            case TypeIndex::Map: {
                // ClickHouse Map is stored as Array of (key, value) tuples
                const auto & map_field = field.safeGet<Map>();
                auto * map_literal = literal->mutable_map();
                const auto * map_type = checkAndGetDataType<DataTypeMap>(type.get());
                if (!map_type)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Map type");

                for (const auto & kv : map_field)
                {
                    const auto & kv_tuple = kv.safeGet<Tuple>();
                    auto * key_value = map_literal->add_key_values();
                    convertFieldToLiteral(kv_tuple[0], map_type->getKeyType(), key_value->mutable_key());
                    convertFieldToLiteral(kv_tuple[1], map_type->getValueType(), key_value->mutable_value());
                }
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Field type {} not yet supported for Substrait literal conversion",
                    type->getName());
        }
    }

    // Extension registry to track function references
    struct FunctionExtension
    {
        String urn;
        String name;
        int reference_id;
    };

    std::vector<FunctionExtension> function_extensions_;
    std::unordered_map<String, int> function_name_to_ref_;
    int next_function_ref_ = 0;

    /// Map ClickHouse function names to Substrait standard names
    static String toSubstraitFunctionName(const String & clickhouse_name)
    {
        if (clickhouse_name == "plus")
            return "add";
        if (clickhouse_name == "minus")
            return "subtract";
        // Other arithmetic types have the same name in ClickHouse and Substrait

        return clickhouse_name;
    }

    // Register a function and get its reference ID
    int registerFunction(const String & function_name)
    {
        // Check if already registered using ClickHouse name as key
        auto it = function_name_to_ref_.find(function_name);
        if (it != function_name_to_ref_.end())
            return it->second;

        // Register new function
        int ref_id = next_function_ref_++;

        // Map ClickHouse function name to Substrait standard name
        String substrait_name = toSubstraitFunctionName(function_name);

        // Use standard Substrait function URNs (format: extension:<OWNER>:<ID>)
        String urn;
        if (function_name == "equals" || function_name == "notEquals" || function_name == "less"
            || function_name == "lessOrEquals" || function_name == "greater" || function_name == "greaterOrEquals")
        {
            urn = "extension:substrait:functions_comparison";
        }
        else if (function_name == "and" || function_name == "or" || function_name == "not")
        {
            urn = "extension:substrait:functions_boolean";
        }
        else if (
            function_name == "plus" || function_name == "minus" || function_name == "multiply"
            || function_name == "divide")
        {
            urn = "extension:substrait:functions_arithmetic";
        }
        else if (
            function_name == "sum" || function_name == "avg" || function_name == "count" || function_name == "min"
            || function_name == "max")
        {
            urn = "extension:substrait:functions_aggregate_generic";
        }
        else
        {
            // Custom ClickHouse function
            urn = "extension:clickhouse:functions";
        }

        function_extensions_.push_back({urn, substrait_name, ref_id});
        function_name_to_ref_[function_name] = ref_id;

        return ref_id;
    }

    /// Find column index by name in a Block header, throws if not found
    int findColumnIndex(const Block & header, const String & column_name, const String & context_message) const
    {
        for (size_t i = 0; i < header.columns(); ++i)
        {
            if (header.getByPosition(i).name == column_name)
                return static_cast<int>(i);
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} not found in input header", context_message);
    }

    /// Build a field selection expression for the given field index
    void buildFieldSelection(substrait::Expression * expr, int field_index) const
    {
        auto * selection = expr->mutable_selection();
        selection->mutable_root_reference();
        auto * direct_ref = selection->mutable_direct_reference();
        direct_ref->mutable_struct_field()->set_field(field_index);
    }

    /// Build a field selection expression and return it
    substrait::Expression makeFieldSelection(int field_index) const
    {
        substrait::Expression expr;
        buildFieldSelection(&expr, field_index);
        return expr;
    }

    // Add extensions to the plan using extension_urns
    void addExtensionsToPlan(substrait::Plan & substrait_plan)
    {
        // Group functions by URN
        std::unordered_map<String, std::vector<const FunctionExtension *>> urn_to_functions;
        for (const auto & ext : function_extensions_)
        {
            urn_to_functions[ext.urn].push_back(&ext);
        }

        // Create extension URN declarations using extension_urns
        std::unordered_map<String, uint32_t> urn_to_anchor;
        uint32_t anchor = 0;
        for (const auto & [urn, functions] : urn_to_functions)
        {
            auto * extension_urn = substrait_plan.add_extension_urns();
            extension_urn->set_extension_urn_anchor(anchor);
            extension_urn->set_urn(urn);
            urn_to_anchor[urn] = anchor;
            ++anchor;
        }

        // Create function extension declarations
        for (const auto & ext : function_extensions_)
        {
            auto * extension = substrait_plan.add_extensions();
            auto * func_ext = extension->mutable_extension_function();
            func_ext->set_extension_urn_reference(urn_to_anchor[ext.urn]);
            func_ext->set_function_anchor(static_cast<uint32_t>(ext.reference_id));
            func_ext->set_name(ext.name);
        }
    }

public:
    explicit Impl() = default;

    void convertPlan(const QueryPlan & query_plan, substrait::Plan & substrait_plan)
    {
        if (!query_plan.isInitialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize uninitialized query plan to Substrait");

        // Clear extension registry for new plan
        function_extensions_.clear();
        function_name_to_ref_.clear();
        next_function_ref_ = 0;

        // Set Substrait version
        auto * version = substrait_plan.mutable_version();
        version->set_major_number(0);
        version->set_minor_number(57);
        version->set_patch_number(1);
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

        const auto & final_header = root->step->getOutputHeader();
        for (const auto & column : *final_header)
        {
            root_rel->add_names(column.name);
        }

        // Add all registered function extensions to the plan
        addExtensionsToPlan(substrait_plan);
    }

private:
    /// Convert ClickHouse DataType to Substrait Type
    void convertType(
        const DataTypePtr & ch_type,
        substrait::Type * substrait_type,
        substrait::Type::Nullability nullability = substrait::Type::NULLABILITY_REQUIRED)
    {
        // unwrap and recurse with NULLABILITY_NULLABLE
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(ch_type.get()))
        {
            convertType(nullable->getNestedType(), substrait_type, substrait::Type::NULLABILITY_NULLABLE);
            return;
        }
        if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(ch_type.get()))
        {
            convertType(lc_type->getDictionaryType(), substrait_type, nullability);
            return;
        }

        switch (ch_type->getTypeId())
        {
            case TypeIndex::Nothing:
                // Nothing type (bare NULL) - map to nullable i32 as Substrait doesn't have a "nothing" type
                substrait_type->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
                break;
            case TypeIndex::Int8:
                substrait_type->mutable_i8()->set_nullability(nullability);
                break;
            case TypeIndex::Int16:
                substrait_type->mutable_i16()->set_nullability(nullability);
                break;
            case TypeIndex::Int32:
                substrait_type->mutable_i32()->set_nullability(nullability);
                break;
            case TypeIndex::Int64:
                substrait_type->mutable_i64()->set_nullability(nullability);
                break;
            case TypeIndex::String:
                substrait_type->mutable_string()->set_nullability(nullability);
                break;
            case TypeIndex::UInt8:
                // Bool is represented as UInt8
                if (ch_type->getName() == "Bool")
                    substrait_type->mutable_bool_()->set_nullability(nullability);
                else
                    // Substrait doesn't have unsigned types, map to signed with larger width
                    substrait_type->mutable_i16()->set_nullability(nullability);
                break;
            case TypeIndex::UInt16:
                substrait_type->mutable_i32()->set_nullability(nullability);
                break;
            case TypeIndex::UInt32:
                substrait_type->mutable_i64()->set_nullability(nullability);
                break;
            case TypeIndex::UInt64:
                // May lose precision for large values
                substrait_type->mutable_i64()->set_nullability(nullability);
                break;
            case TypeIndex::Float32:
                substrait_type->mutable_fp32()->set_nullability(nullability);
                break;
            case TypeIndex::Float64:
                substrait_type->mutable_fp64()->set_nullability(nullability);
                break;
            case TypeIndex::BFloat16:
                substrait_type->mutable_fp32()->set_nullability(nullability);
                break;
            case TypeIndex::Date: {
                // Days since Unix epoch
                substrait_type->mutable_date()->set_nullability(nullability);
                break;
            }
            case TypeIndex::DateTime: {
                // ClickHouse timestamp is in seconds, Substrait's in microseconds
                // Explicitly set precision to 6
                auto * ts = substrait_type->mutable_precision_timestamp();
                ts->set_precision(6);
                ts->set_nullability(nullability);
                break;
            }
            case TypeIndex::DateTime64: {
                auto * ts = substrait_type->mutable_precision_timestamp();
                const auto * dt64 = checkAndGetDataType<DataTypeDateTime64>(ch_type.get());
                if (!dt64)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Expected DateTime64 type but got {}", ch_type->getName());
                ts->set_precision(dt64->getScale());
                ts->set_nullability(nullability);
                break;
            }
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::Decimal256: {
                auto * decimal_type = substrait_type->mutable_decimal();
                decimal_type->set_nullability(nullability);
                decimal_type->set_scale(getDecimalScale(*ch_type));
                decimal_type->set_precision(getDecimalPrecision(*ch_type));
                break;
            }
            case TypeIndex::Array: {
                auto * list = substrait_type->mutable_list();
                list->set_nullability(nullability);
                // Recursively convert the nested element type
                const auto * array_type = checkAndGetDataType<DataTypeArray>(ch_type.get());
                convertType(array_type->getNestedType(), list->mutable_type());
                break;
            }
            case TypeIndex::Tuple: {
                auto * strct = substrait_type->mutable_struct_();
                strct->set_nullability(nullability);
                const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(ch_type.get());
                for (const auto & element : tuple_type->getElements())
                {
                    // Add each tuple element to the Substrait struct types
                    convertType(element, strct->add_types());
                }
                break;
            }
            case TypeIndex::Map: {
                auto * map = substrait_type->mutable_map();
                map->set_nullability(nullability);
                const auto * ch_map = checkAndGetDataType<DataTypeMap>(ch_type.get());
                convertType(ch_map->getKeyType(), map->mutable_key());
                convertType(ch_map->getValueType(), map->mutable_value());
                break;
            }
            case TypeIndex::AggregateFunction: {
                const auto * agg_func_type = checkAndGetDataType<DataTypeAggregateFunction>(ch_type.get());
                if (!agg_func_type)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected AggregateFunction type");
                convertType(agg_func_type->getFunction()->getResultType(), substrait_type, nullability);
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Type {} not yet supported for Substrait conversion",
                    ch_type->getName());
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
            case ActionsDAG::ActionType::INPUT: {
                // Column reference - find column index in input
                int field_index = findColumnIndex(
                    input_header, node->result_name, fmt::format("Column {}", node->result_name));
                buildFieldSelection(&expr, field_index);
                break;
            }

            case ActionsDAG::ActionType::COLUMN: {
                // Constant value
                auto * literal = expr.mutable_literal();

                if (!node->column || node->column->empty() || (*node->column)[0].isNull())
                {
                    auto * null_type = literal->mutable_null();
                    convertType(node->result_type, null_type);
                }
                else
                {
                    // Delegate to convertFieldToLiteral for all types
                    convertFieldToLiteral((*node->column)[0], node->result_type, literal);
                }
                break;
            }

            case ActionsDAG::ActionType::ALIAS: {
                // Just pass through to the child
                if (node->children.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "ALIAS node must have a child");
                return convertExpression(node->children[0], input_header);
            }

            case ActionsDAG::ActionType::FUNCTION: {
                const String & func_name = node->function_base->getName();
                auto * scalar_func = expr.mutable_scalar_function();

                int ref_id = registerFunction(func_name);
                scalar_func->set_function_reference(ref_id);

                convertType(node->result_type, scalar_func->mutable_output_type());

                for (const auto * child : node->children)
                {
                    auto * arg = scalar_func->add_arguments();
                    arg->mutable_value()->CopyFrom(convertExpression(child, input_header));
                }
                break;
            }

            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
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

        if (step_name.starts_with("ReadFrom"))
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
        else if (step_name == "MergingAggregated")
        {
            convertMergingAggregatedStep(node, &rel);
        }
        else if (step_name == "Join")
        {
            convertJoinStep(node, &rel);
        }
        else if (step_name == "Limit")
        {
            convertLimitStep(node, &rel);
        }
        else if (step_name == "Distinct" || step_name == "PreDistinct")
        {
            convertDistinctStep(node, &rel);
        }
        else if (step_name == "Union")
        {
            convertUnionStep(node, &rel);
        }
        else if (step_name == "IntersectOrExcept")
        {
            convertIntersectOrExceptStep(node, &rel);
        }
        else
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Step type '{}' not yet supported for Substrait conversion", step_name);
        }

        return rel;
    }

    void convertReadStep(const IQueryPlanStep & step, substrait::Rel * rel)
    {
        auto * read_rel = rel->mutable_read();

        // Get output header to determine schema
        const auto & header = *step.getOutputHeader();
        auto * base_schema = read_rel->mutable_base_schema();

        base_schema->mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);

        // Convert columns to Substrait schema
        for (const auto & column : header)
        {
            base_schema->add_names(column.name);
            convertType(column.type, base_schema->mutable_struct_()->add_types());
        }

        // Create a named table reference
        auto * named_table = read_rel->mutable_named_table();
        String table_name = "";

        // Determine table name based on step type
        if (const auto * read_from_mt = typeid_cast<const ReadFromMergeTree *>(&step))
        {
            StorageID storage_id = read_from_mt->getStorageID();
            table_name = storage_id.getTableName();
        }
        else if (const auto * read_from_memory = typeid_cast<const ReadFromMemoryStorageStep *>(&step))
        {
            StorageID storage_id = read_from_memory->getStorage()->getStorageID();
            table_name = storage_id.getTableName();
        }
        else if (const auto * read_from_storage = typeid_cast<const ReadFromStorageStep *>(&step))
        {
            StorageID storage_id = read_from_storage->getStorage()->getStorageID();
            table_name = storage_id.getTableName();
        }
        else if (const auto * read_from_table = typeid_cast<const ReadFromTableStep *>(&step))
        {
            table_name = read_from_table->getTable();
        }

        // Add table name to named_table when known. For constant/virtual sources
        // (e.g. SELECT 1), Substrait allows ReadRel without a named table, so
        // use virtual_source as table name in this case.
        if (!table_name.empty())
            named_table->add_names(table_name);
        else
            named_table->add_names("virtual_source");
    }

    void convertFilterStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter step must have a child");

        auto * filter_rel = rel->mutable_filter();

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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Filter column {} not found in ActionsDAG outputs", filter_column_name);

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

        // Cast to ExpressionStep to access the ActionsDAG
        const auto * expr_step = dynamic_cast<const ExpressionStep *>(node->step.get());
        if (!expr_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ExpressionStep but got {}", node->step->getName());

        // Get the ActionsDAG containing the expressions
        const auto & actions_dag = expr_step->getExpression();

        // Get input header from child node
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;

        auto child_rel = convertNode(node->children[0]);

        // ExpressionStep always produces ProjectRel
        auto * project_rel = rel->mutable_project();
        project_rel->mutable_input()->Swap(&child_rel);

        // Convert all output expressions
        const auto & outputs = actions_dag.getOutputs();
        for (const auto * output_node : outputs)
        {
            auto expr = convertExpression(output_node, input_header);
            project_rel->add_expressions()->Swap(&expr);
        }

        // Substrait ProjectRel outputs input_fields + expression_fields
        // ClickHouse ExpressionStep outputs only the final columns
        // Emit mapping to select only the expression outputs
        size_t input_field_count = input_header.columns();
        size_t output_count = outputs.size();
        auto * emit = project_rel->mutable_common()->mutable_emit();
        for (size_t i = 0; i < output_count; ++i)
        {
            emit->add_output_mapping(static_cast<int32_t>(input_field_count + i));
        }
    }

    void convertSortingStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting step must have a child");

        auto * sort_rel = rel->mutable_sort();

        auto child_rel = convertNode(node->children[0]);
        sort_rel->mutable_input()->Swap(&child_rel);

        // Cast to SortingStep to access sort description
        const auto * sorting_step = dynamic_cast<const SortingStep *>(node->step.get());
        if (!sorting_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected SortingStep but got {}", node->step->getName());

        // Get sort description
        const auto & sort_description = sorting_step->getSortDescription();

        // Get input header to map column names to indices
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;

        // Convert each sort column
        for (const auto & sort_col : sort_description)
        {
            auto * sort_field = sort_rel->add_sorts();

            int field_index = findColumnIndex(
                input_header, sort_col.column_name, fmt::format("Sort column {}", sort_col.column_name));

            buildFieldSelection(sort_field->mutable_expr(), field_index);

            // Map sort direction and nulls handling
            // direction: 1 = ASC, -1 = DESC
            // nulls_direction: 1 = NULLS LAST (when direction=1) or NULLS FIRST (when direction=-1)
            //                 -1 = NULLS FIRST (when direction=1) or NULLS LAST (when direction=-1)
            if (sort_col.direction == 1) // ASC
            {
                if (sort_col.nulls_direction == 1) // NULLS LAST
                    sort_field->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);
                else // NULLS FIRST
                    sort_field->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);
            }
            else // DESC
            {
                if (sort_col.nulls_direction == -1) // NULLS LAST
                    sort_field->set_direction(substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST);
                else // NULLS FIRST
                    sort_field->set_direction(substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST);
            }
        }
    }

    void convertAggregatingStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregating step must have a child");

        auto * agg_rel = rel->mutable_aggregate();

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
            int field_index = findColumnIndex(input_header, key, fmt::format("Grouping key {}", key));

            auto * grouping_expr = agg_rel->add_groupings()->add_grouping_expressions();
            buildFieldSelection(grouping_expr, field_index);
        }

        // Convert aggregate functions
        for (const auto & aggregate : params.aggregates)
        {
            auto * measure = agg_rel->add_measures();
            auto * agg_func = measure->mutable_measure();

            // Register aggregate function and get reference ID (will be added to extension_urns)
            const String & func_name = aggregate.function->getName();
            int ref_id = registerFunction(func_name);
            agg_func->set_function_reference(ref_id);

            convertType(aggregate.function->getResultType(), agg_func->mutable_output_type());

            // Convert aggregate arguments
            for (const auto & arg_column : aggregate.argument_names)
            {
                int field_index = findColumnIndex(
                    input_header, arg_column, fmt::format("Aggregate argument {}", arg_column));

                auto * arg = agg_func->add_arguments();
                buildFieldSelection(arg->mutable_value(), field_index);
            }
        }
    }

    void convertJoinStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.size() < 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join step must have at least two children");

        const auto join_step = dynamic_cast<const JoinStep *>(node->step.get());

        // Cast to JoinStep to access join information
        if (!join_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected JoinStep but got {}", node->step->getName());

        const JoinPtr & join = join_step->getJoin();
        if (!join)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep has no join algorithm set");

        const TableJoin & table_join = join->getTableJoin();
        const auto kind = table_join.kind();

        // Handle CROSS JOIN as CrossRel
        if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        {
            auto * cross_rel = rel->mutable_cross();
            auto left_child_rel = convertNode(node->children[0]);
            auto right_child_rel = convertNode(node->children[1]);
            cross_rel->mutable_left()->Swap(&left_child_rel);
            cross_rel->mutable_right()->Swap(&right_child_rel);
            return;
        }

        // Standard JoinRel
        auto * join_rel = rel->mutable_join();

        // Convert children nodes
        auto left_child_rel = convertNode(node->children[0]);
        auto right_child_rel = convertNode(node->children[1]);
        join_rel->mutable_left()->Swap(&left_child_rel);
        join_rel->mutable_right()->Swap(&right_child_rel);

        // Get headers to resolve column indices
        const auto & left_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & left_header = *left_header_ptr;
        const auto & right_header_ptr = node->children[1]->step->getOutputHeader();
        const Block & right_header = *right_header_ptr;
        size_t left_fields = left_header.columns();

        // Build join condition from TableJoin clauses
        const auto & clauses = table_join.getClauses();
        std::vector<substrait::Expression> clause_expressions;

        for (const auto & clause : clauses)
        {
            std::vector<substrait::Expression> key_conjuncts;

            // Build equality conditions for each join key pair
            for (size_t i = 0; i < clause.key_names_left.size(); ++i)
            {
                const auto & left_key = clause.key_names_left[i];
                const auto & right_key = clause.key_names_right[i];

                int left_idx = findColumnIndex(left_header, left_key, fmt::format("Join key {}", left_key));
                int right_idx = findColumnIndex(right_header, right_key, fmt::format("Join key {}", right_key));

                // Create equality expression: left_key = right_key
                // Right fields are offset by left_fields in the joined tuple
                auto left_sel = makeFieldSelection(left_idx);
                auto right_sel = makeFieldSelection(static_cast<int>(left_fields + right_idx));

                substrait::Expression eq_expr;
                auto * scalar_func = eq_expr.mutable_scalar_function();
                int ref_id = registerFunction("equals");
                scalar_func->set_function_reference(ref_id);

                auto * arg0 = scalar_func->add_arguments();
                arg0->mutable_value()->CopyFrom(left_sel);
                auto * arg1 = scalar_func->add_arguments();
                arg1->mutable_value()->CopyFrom(right_sel);

                key_conjuncts.push_back(std::move(eq_expr));
            }

            // Combine multiple key conditions with AND
            if (!key_conjuncts.empty())
            {
                if (key_conjuncts.size() == 1)
                {
                    clause_expressions.push_back(std::move(key_conjuncts[0]));
                }
                else
                {
                    substrait::Expression and_expr;
                    auto * and_func = and_expr.mutable_scalar_function();
                    int and_ref = registerFunction("and");
                    and_func->set_function_reference(and_ref);

                    for (const auto & conj : key_conjuncts)
                    {
                        auto * arg = and_func->add_arguments();
                        arg->mutable_value()->CopyFrom(conj);
                    }

                    clause_expressions.push_back(std::move(and_expr));
                }
            }
        }

        // Set join condition (combine clauses with OR if multiple)
        if (!clause_expressions.empty())
        {
            if (clause_expressions.size() == 1)
            {
                join_rel->mutable_expression()->CopyFrom(clause_expressions[0]);
            }
            else
            {
                substrait::Expression or_expr;
                auto * or_func = or_expr.mutable_scalar_function();
                int or_ref = registerFunction("or");
                or_func->set_function_reference(or_ref);

                for (const auto & clause_expr : clause_expressions)
                {
                    auto * arg = or_func->add_arguments();
                    arg->mutable_value()->CopyFrom(clause_expr);
                }

                join_rel->mutable_expression()->CopyFrom(or_expr);
            }
        }

        // Map ClickHouse JoinKind and JoinStrictness to Substrait JoinType
        const auto strictness = table_join.strictness();
        substrait::JoinRel::JoinType join_type = substrait::JoinRel::JOIN_TYPE_INNER;

        if (kind == JoinKind::Inner)
        {
            join_type = substrait::JoinRel::JOIN_TYPE_INNER;
        }
        else if (kind == JoinKind::Left)
        {
            if (strictness == JoinStrictness::Semi)
                join_type = substrait::JoinRel::JOIN_TYPE_LEFT_SEMI;
            else if (strictness == JoinStrictness::Anti)
                join_type = substrait::JoinRel::JOIN_TYPE_LEFT_ANTI;
            else
                join_type = substrait::JoinRel::JOIN_TYPE_LEFT;
        }
        else if (kind == JoinKind::Right)
        {
            if (strictness == JoinStrictness::Semi)
                join_type = substrait::JoinRel::JOIN_TYPE_RIGHT_SEMI;
            else if (strictness == JoinStrictness::Anti)
                join_type = substrait::JoinRel::JOIN_TYPE_RIGHT_ANTI;
            else
                join_type = substrait::JoinRel::JOIN_TYPE_RIGHT;
        }
        else if (kind == JoinKind::Full)
        {
            join_type = substrait::JoinRel::JOIN_TYPE_OUTER;
        }

        join_rel->set_type(join_type);

        // TODO: Support ASOF joins, post-join filters, and other advanced join features
    }

    void convertLimitStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Limit step must have a child");

        auto * fetch_rel = rel->mutable_fetch();

        auto child_rel = convertNode(node->children[0]);
        fetch_rel->mutable_input()->Swap(&child_rel);

        // Cast to LimitStep to access limit/offset
        const auto * limit_step = dynamic_cast<const LimitStep *>(node->step.get());
        if (!limit_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected LimitStep");

        // Get limit (count) and offset
        size_t limit = limit_step->getLimit();
        size_t offset = limit_step->getOffset();

        fetch_rel->set_count(static_cast<int64_t>(limit));
        if (offset > 0)
            fetch_rel->set_offset(static_cast<int64_t>(offset));
    }

    void convertDistinctStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Distinct step must have a child");

        auto * agg_rel = rel->mutable_aggregate();

        auto child_rel = convertNode(node->children[0]);
        agg_rel->mutable_input()->Swap(&child_rel);

        // Cast to DistinctStep to access distinct columns
        const auto * distinct_step = dynamic_cast<const DistinctStep *>(node->step.get());
        if (!distinct_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected DistinctStep");

        // Get input header from child node
        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;

        // Get the distinct columns
        const auto & columns = distinct_step->getColumnNames();
        auto * grouping = agg_rel->add_groupings();

        if (columns.empty())
        {
            // DISTINCT on all columns
            for (size_t i = 0; i < input_header.columns(); ++i)
            {
                auto * grouping_expr = grouping->add_grouping_expressions();
                buildFieldSelection(grouping_expr, static_cast<int>(i));
            }
        }
        else
        {
            // DISTINCT on specific columns
            for (const auto & col_name : columns)
            {
                int field_index = findColumnIndex(input_header, col_name, fmt::format("Distinct column {}", col_name));

                auto * grouping_expr = grouping->add_grouping_expressions();
                buildFieldSelection(grouping_expr, field_index);
            }
        }
        // No measures - this produces distinct rows via grouping only
    }

    void convertMergingAggregatedStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MergingAggregated step must have a child");

        auto * agg_rel = rel->mutable_aggregate();

        auto child_rel = convertNode(node->children[0]);
        agg_rel->mutable_input()->Swap(&child_rel);

        const auto * merging_step = dynamic_cast<const MergingAggregatedStep *>(node->step.get());
        if (!merging_step)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Expected MergingAggregatedStep but got {}", node->step->getName());

        const auto & params = merging_step->getParams();

        const auto & input_header_ptr = node->children[0]->step->getOutputHeader();
        const Block & input_header = *input_header_ptr;

        // Convert grouping keys
        for (const auto & key : params.keys)
        {
            int field_index = findColumnIndex(input_header, key, fmt::format("Grouping key {}", key));

            auto * grouping_expr = agg_rel->add_groupings()->add_grouping_expressions();
            buildFieldSelection(grouping_expr, field_index);
        }

        // Convert aggregate functions
        for (const auto & aggregate : params.aggregates)
        {
            auto * measure = agg_rel->add_measures();
            auto * agg_func = measure->mutable_measure();

            const String & func_name = aggregate.function->getName();
            int ref_id = registerFunction(func_name);
            agg_func->set_function_reference(ref_id);

            convertType(aggregate.function->getResultType(), agg_func->mutable_output_type());

            int field_index = findColumnIndex(
                input_header, aggregate.column_name, fmt::format("Aggregate column {}", aggregate.column_name));

            auto * arg = agg_func->add_arguments();
            buildFieldSelection(arg->mutable_value(), field_index);
        }
    }

    void convertUnionStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Union step must have children");

        auto * set_rel = rel->mutable_set();
        set_rel->set_op(substrait::SetRel::SET_OP_UNION_ALL);

        // Convert all child nodes
        for (const auto * child : node->children)
        {
            auto child_rel = convertNode(child);
            set_rel->add_inputs()->Swap(&child_rel);
        }
    }

    void convertIntersectOrExceptStep(const QueryPlan::Node * node, substrait::Rel * rel)
    {
        if (node->children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IntersectOrExcept step must have children");

        const auto * intersect_except_step = dynamic_cast<const IntersectOrExceptStep *>(node->step.get());
        if (!intersect_except_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected IntersectOrExceptStep");

        auto * set_rel = rel->mutable_set();

        // Map the operator to Substrait SetRel operation
        using Operator = ASTSelectIntersectExceptQuery::Operator;
        switch (intersect_except_step->getOperator())
        {
            case Operator::INTERSECT_ALL:
                // Substrait doesn't have INTERSECT ALL directly, use PRIMARY (keeps duplicates from first input)
                set_rel->set_op(substrait::SetRel::SET_OP_INTERSECTION_PRIMARY);
                break;
            case Operator::INTERSECT_DISTINCT:
                // INTERSECT DISTINCT removes duplicates
                set_rel->set_op(substrait::SetRel::SET_OP_INTERSECTION_PRIMARY);
                break;
            case Operator::EXCEPT_ALL:
                // EXCEPT ALL (keeps duplicates)
                set_rel->set_op(substrait::SetRel::SET_OP_MINUS_PRIMARY);
                break;
            case Operator::EXCEPT_DISTINCT:
                // EXCEPT DISTINCT removes duplicates
                set_rel->set_op(substrait::SetRel::SET_OP_MINUS_PRIMARY);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown IntersectOrExcept operator");
        }

        // Convert all child nodes
        for (const auto * child : node->children)
        {
            auto child_rel = convertNode(child);
            set_rel->add_inputs()->Swap(&child_rel);
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to serialize Substrait plan to JSON: {}",
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
