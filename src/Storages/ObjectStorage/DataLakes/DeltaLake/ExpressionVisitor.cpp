#include "ExpressionVisitor.h"
#include "Functions/FunctionFactory.h"

#if USE_DELTA_KERNEL_RS

#include <Analyzer/FunctionNode.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeMap.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/tuple.h>
#include <Functions/isNull.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>

#include <Common/DateLUTImpl.h>
#include <Common/LocalDate.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnNullable.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>

#include <fmt/ranges.h>
#include "KernelUtils.h"
#include "delta_kernel_ffi.hpp"


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DeltaLake
{
/// delta-kernel-rs provides ExpressionVisitor API.
/// It requires for us to implement visitor functions:
/// 1. Literal visitors: visitStringLiteral, visitDateTimeLiteral, etc
/// 2. Identifier visitor: visitColumnExpression.
/// 3. Operator visitors: visitIn, visitAnd, visitGreater, etc
/// 4. Function visitors: visitAdd, etc
///
/// This ExpressionVisitor API uses "lists" to represent the expression structure.
/// To explain what it is, let's take this simple example,
/// which we will have in ScanCallback's transform expression,
/// when a table only has ordinary (a, b, d) and partition columns (c - Array(DateLiteral()), e - Tuple(String, Array(Int))):
/// structExpression(
///     ColumnExpr(a),
///     ColumnExpr(b),
///     ArrayLiteral(DateLiteral('2025-12-12'), DateLiteral('2025-01-01')),
///     ColumnExpr(d),
///     TupleLiteral(StringLiteral('Amsterdam'), ArrayLiteral(IntLiteral(12), IntLiteral(1), IntLiteral(2))))
/// So in delta-kernel's "lists" it will look like:
///
///                                       [StructExpression(child_list_id = 1)] -- list_id = 0
///                                                         |
///                                                         |
/// [ColumnExpr(a), ColumnExpr(b), ArrayLiteral(child_list_id = 2), ColumnExpr(c), TupleLiteral(child_list_id = 3)] <-- list_id = 1
///                                     |                              |
///                                     |                              |
///            DateLiteral('2025-12-12'), DateLiteral('2025-01-01')] <----- list_id = 2
///                                                                    |
///                                          [StringLiteral('Amstedam'), ArrayLiteral(child_list_id = 4)] <-- list_id = 3
///                                                                            |
///                                                                            |
///                                                      [IntLiteral(12), IntLiteral{1}, IntLiteral(2)] <-- list_id = 4

/// ExpressionVisitorData holds a state of ExpressionVisitor.
class ExpressionVisitorData
{
private:
    LoggerPtr log = getLogger("DeltaLakeExpressionVisitor");
    /// A counter for expression node lists,
    /// which represent an intermediate parsing result.
    size_t list_counter = 0;
    /// Counter used to form const column names
    /// as temporary names for constant columns at first.
    /// Actual names will be assigned to them once the result expression is formed.
    size_t literal_counter = 0;
    /// Result expression schema.
    const DB::NamesAndTypesList & schema;

    /// Final parsing result.
    std::shared_ptr<DB::ActionsDAG> dag;
    /// Intermediate parsing result.
    std::map<size_t, DB::ActionsDAG::NodeRawConstPtrs> node_lists;
    /// First exception thrown from visitor functions.
    std::exception_ptr visitor_exception;

    DB::ContextPtr context; /// TODO: pass query context

public:
    /// `schema` is the expression schema of result expression.
    explicit ExpressionVisitorData(const DB::NamesAndTypesList & schema_)
        : schema(schema_)
        , dag(std::make_shared<DB::ActionsDAG>())
        , context(DB::Context::getGlobalContextInstance())
    {
    }

    DB::ContextPtr getContext() const { return context; }

    /// Get result of a parsed scanCallback expression.
    /// Its result is always a Tuple.
    std::shared_ptr<DB::ActionsDAG> getScanCallbackExpressionResult()
    {
        /// In the process of parsing `node_lists` can have size > 1,
        /// but once parsing is finished -
        /// it must have been formed into a single list with a single element.
        if (node_lists.size() != 1)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of a result expression: {}",
                node_lists.size());
        }
        if (node_lists[0].size() != 1)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of a result expression at root node: {}",
                node_lists[0].size());
        }

        const auto & nodes = node_lists[0][0]->children;
        if (nodes.size() != schema.size())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of expression list: {} (expected: {})",
                nodes.size(), schema.size());
        }

        auto schema_it = schema.begin();

        DB::ColumnsWithTypeAndName result_columns;

        /// Finalize the result in result_dag with requested schema.
        for (const auto & node : nodes)
        {
            LOG_TEST(log, "Node type: {}, result name: {}", node->type, node->result_name);

            /// During parsing we assigned temporary const_{i} names
            /// to constant expressions,
            /// because we do not know their names at the moment of parsing,
            /// but once the result expression is formed -
            /// its schema must conform with `schema` passed to constructor of ExpressionVisitorData
            /// (only nullability of types can differ,
            /// because when we parse non-null values, they are assigned non-nullable types).
            /// So we substitute constant column names here.
            DB::ColumnWithTypeAndName column_with_type_and_name;
            if (node->type == DB::ActionsDAG::ActionType::COLUMN)
            {
                DB::ColumnPtr column;
                DB::DataTypePtr type;
                if (schema_it->type->isNullable())
                {
                    type = DB::makeNullable(node->result_type);
                    column = DB::makeNullable(node->column);
                }
                else
                {
                    type = node->result_type;
                    column = node->column;
                }
                column_with_type_and_name = DB::ColumnWithTypeAndName(column, type, schema_it->name);
            }
            else
            {
                column_with_type_and_name = DB::ColumnWithTypeAndName(node->column, node->result_type, node->result_name);
            }

            LOG_TEST(
                log, "Added output: {}, type: {}",
                column_with_type_and_name.name, column_with_type_and_name.type->getTypeId());

            result_columns.push_back(column_with_type_and_name);
            ++schema_it;
        }

        auto result_dag = std::make_shared<DB::ActionsDAG>(result_columns);

        DB::FunctionOverloadResolverPtr function =
            std::make_unique<DB::FunctionToOverloadResolverAdaptor>(
                std::make_shared<DB::FunctionTuple>());
        result_dag->addFunction(function, result_dag->getOutputs(), {});

        return result_dag;
    }

    const LoggerPtr & logger() const { return log; }

    /// Get (the first) exception, which happened during parsing.
    std::exception_ptr getException() const { return visitor_exception; }

    /// Set parsing expression.
    void setException(std::exception_ptr exception)
    {
        if (!visitor_exception)
            visitor_exception = exception;
    }

    /// Create a new node list and return its id.
    size_t makeNewList(size_t capacity_hint)
    {
        size_t id = list_counter++;
        auto [it, inserted] = node_lists.emplace(id, DB::ActionsDAG::NodeRawConstPtrs{});
        chassert(inserted);
        if (capacity_hint > 0)
            it->second.reserve(capacity_hint);
        return id;
    }

    /// Add literal (constant) value to the list by `list_id`.
    void addLiteral(size_t list_id, DB::Field value, DB::DataTypePtr type)
    {
        chassert(type);
        auto col = type->createColumnConst(1, value);
        auto column = DB::ColumnWithTypeAndName(
            col,
            type,
            /* name */"const_" + DB::toString(literal_counter++));

        const auto & node = dag->addColumn(std::move(column));

        node_lists[list_id].push_back(&node);
        LOG_TEST(log, "Added list id {}", list_id);
    }

    /// Add identifier (column name) node to the list by `list_id`.
    void addIdentifier(size_t list_id, const std::string & name)
    {
        std::string column_name;
        if (name.starts_with("`") && name.ends_with("`"))
            column_name = name.substr(1, name.size() - 2);
        else
            column_name = name;

        auto name_and_type = schema.tryGetByName(column_name);
        if (!name_and_type.has_value())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Cannot find column {} in schema ({})",
                name, schema.toString());
        }

        auto column = DB::ColumnWithTypeAndName(
            name_and_type->type->createColumn(),
            name_and_type->type,
            name_and_type->name);

        const auto & node = dag->addInput(std::move(column));

        node_lists[list_id].push_back(&node);
        LOG_TEST(log, "Added list id {}", list_id);
    }

    /// Add function node to the list by `list_id`.
    /// `child_list_id` is the id of the list which contains function arguments.
    /// So we will extract that child list, remove it from node_lists
    /// and insert back as a part of FunctionNode.
    void addFunction(size_t list_id, size_t child_list_id, DB::FunctionOverloadResolverPtr function)
    {
        auto it = node_lists.find(child_list_id);
        if (it == node_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Cannot find child list id {}", child_list_id);
        }

        const auto & node = dag->addFunction(function, std::move(it->second), {});

        node_lists.erase(child_list_id);
        LOG_TEST(log, "Removed list id {}", child_list_id);

        node_lists[list_id].push_back(&node);
        LOG_TEST(log, "Added list id {}", list_id);
    }

    /// Once a list by id `list_id` is fully formed
    /// and if this list fully contains literal (constant) arguments,
    /// we might use this list as whole to construct Array or Tuple elements.
    /// In this case we need to extract this list from the node_lists and remove it,
    /// as afterwards it will be inserted as a part of Array(Tuple)Literal via addLiteral().
    template <typename ValueContainer>
    std::pair<ValueContainer, DB::DataTypes> extractLiteralList(size_t list_id)
    {
        auto it = node_lists.find(list_id);
        if (it == node_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Cannot find list id {}", list_id);
        }

        ValueContainer values;
        values.reserve(it->second.size());

        DB::DataTypes types;
        types.reserve(it->second.size());

        for (const auto & node : it->second)
        {
            if (node->type != DB::ActionsDAG::ActionType::COLUMN)
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Not a constant column: {} (list id: {})",
                    magic_enum::enum_name(node->type), list_id);
            }

            DB::Field value;
            node->column->get(0, value);
            values.push_back(std::move(value));
            types.push_back(node->result_type);
        }

        node_lists.erase(it);
        LOG_TEST(log, "Removed list id {}", list_id);

        return std::pair(values, types);
    }
};

class ExpressionVisitor
{
public:
    static void visit(const ffi::Expression * expression, ExpressionVisitorData & data)
    {
        auto visitor = createVisitor(data);
        [[maybe_unused]] uintptr_t result = ffi::visit_expression_ref(expression, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (auto e = data.getException())
            std::rethrow_exception(e);
    }

    static void visit(ffi::SharedExpression * expression, ExpressionVisitorData & data)
    {
        auto visitor = createVisitor(data);
        [[maybe_unused]] uintptr_t result = ffi::visit_expression(&expression, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (auto e = data.getException())
            std::rethrow_exception(e);
    }

private:
    /// At this moment ExpressionVisitor is used only for partition columns,
    /// where only identifier expressions are allowed (only PARTITION BY col_name, ...),
    /// therefore we leave several visitor function as not implemented
    /// (see throwNotImplemented in createVisitor() below).
    /// They will be implemented once we start using statistics feature from delta-kernel.
    enum NotImplementedMethod
    {
        LT,
        //LE,
        GT,
        //GE,
        EQ,
        //NE,
        DISTINCT,
        IN,
        //NOT_IN,
        ADD,
        MINUS,
        MULTIPLY,
        DIVIDE,
    };
    static ffi::EngineExpressionVisitor createVisitor(ExpressionVisitorData & data)
    {
        ffi::EngineExpressionVisitor visitor;
        visitor.data = &data;
        visitor.make_field_list = &makeFieldList;

        visitor.visit_literal_bool = &visitSimpleLiteral<bool, DB::DataTypeUInt8>;
        visitor.visit_literal_byte = &visitSimpleLiteral<int8_t, DB::DataTypeInt8>;
        visitor.visit_literal_short = &visitSimpleLiteral<int16_t, DB::DataTypeInt16>;
        visitor.visit_literal_int = &visitSimpleLiteral<int32_t, DB::DataTypeInt32>;
        visitor.visit_literal_long = &visitSimpleLiteral<int64_t, DB::DataTypeInt64>;
        visitor.visit_literal_float = &visitSimpleLiteral<float, DB::DataTypeFloat32>;
        visitor.visit_literal_double = &visitSimpleLiteral<double, DB::DataTypeFloat64>;

        visitor.visit_literal_string = &visitStringLiteral;
        visitor.visit_literal_decimal = &visitDecimalLiteral;

        visitor.visit_literal_timestamp = &visitTimestampLiteral;
        visitor.visit_literal_timestamp_ntz = &visitTimestampNtzLiteral;
        visitor.visit_literal_date = &visitDateLiteral;
        visitor.visit_literal_binary = &visitBinaryLiteral;
        visitor.visit_literal_null = &visitNullLiteral;
        visitor.visit_literal_array = &visitArrayLiteral;
        visitor.visit_literal_struct = &visitStructLiteral;
        visitor.visit_literal_map = &visitMapLiteral;

        visitor.visit_column = &visitColumnExpression;
        visitor.visit_struct_expr = &visitStructExpression;

        visitor.visit_or = &visitFunction<DB::FunctionOr>;
        visitor.visit_and = &visitFunction<DB::FunctionAnd>;
        visitor.visit_not = &visitFunction<DB::FunctionNot>;

        visitor.visit_is_null = &visitFunction<DB::FunctionIsNull>;

        visitor.visit_lt = &throwNotImplemented<LT>;
        visitor.visit_gt = &throwNotImplemented<GT>;
        visitor.visit_eq = &throwNotImplemented<EQ>;
        visitor.visit_distinct = &throwNotImplemented<DISTINCT>;
        visitor.visit_in = &throwNotImplemented<IN>;
        visitor.visit_add = &throwNotImplemented<ADD>;
        visitor.visit_minus = &throwNotImplemented<MINUS>;
        visitor.visit_multiply = &throwNotImplemented<MULTIPLY>;
        visitor.visit_divide = &throwNotImplemented<DIVIDE>;

        return visitor;
    }

    static uintptr_t makeFieldList(void * data, uintptr_t capacity_hint)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        return state->makeNewList(capacity_hint);
    }

    template <typename Func>
    static void visitorImpl(ExpressionVisitorData & data, Func func)
    {
        try
        {
            func();
        }
        catch (...)
        {
            /// We cannot allow to throw exceptions from visitor functions,
            /// otherwise delta-kernel will panic and call terminate.
            data.setException(std::current_exception());
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    template <NotImplementedMethod method>
    static void throwNotImplemented(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_list_id)
    {
        UNUSED(sibling_list_id);
        UNUSED(child_list_id);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED,
                "Method {} not implemented", magic_enum::enum_name(method));
        });
    }

    template <typename Func>
    static void visitFunction(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(
                state->logger(),
                "List id: {}, child list id: {}, type: Function {}",
                sibling_list_id, child_list_id, Func::name);

            DB::FunctionOverloadResolverPtr function = DB::FunctionFactory::instance().get(Func::name, state->getContext());
            state->addFunction(sibling_list_id, child_list_id, std::move(function));
        });
    }

    static void visitColumnExpression(void * data, uintptr_t sibling_list_id, ffi::KernelStringSlice name)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            const auto name_str = KernelUtils::fromDeltaString(name);
            LOG_TEST(state->logger(), "List id: {}, name: {}, type: Column", sibling_list_id, name_str);

            state->addIdentifier(sibling_list_id, name_str);
        });
    }

    static void visitStructExpression(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(
                state->logger(),
                "List id: {}, child list id: {}, type: StructExpression",
                sibling_list_id, child_list_id);


            DB::FunctionOverloadResolverPtr function =
                std::make_unique<DB::FunctionToOverloadResolverAdaptor>(
                    std::make_shared<DB::FunctionTuple>());

            state->addFunction(sibling_list_id, child_list_id, std::move(function));
        });
    }

    template <typename T, typename DataType>
    static void visitSimpleLiteral(void * data, uintptr_t sibling_list_id, T value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: {}", sibling_list_id, DataType::type_id);
            state->addLiteral(sibling_list_id, value, std::make_shared<DataType>());
        });
    }

    static void visitStringLiteral(void * data, uintptr_t sibling_list_id, ffi::KernelStringSlice value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            auto value_str = KernelUtils::fromDeltaString(value);
            visitSimpleLiteral<std::string, DB::DataTypeString>(data, sibling_list_id, value_str);
        });
    }

    static void visitDecimalLiteral(
        void * data,
        uintptr_t sibling_list_id,
        int64_t value_ms,
        uint64_t value_ls,
        uint8_t precision,
        uint8_t scale)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            /// From delta-kernel-rs:
            /// "The 128bit integer
            /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
            /// bits in `value_ls`"
            /// Also in clickhouse decimal is in little endian, so we switch the order for Decimal128.

            DB::Field value;
            if (precision <= DB::DecimalUtils::max_precision<DB::Decimal32>)
            {
                value = DB::DecimalField<DB::Decimal32>(value_ls, scale);
                state->addLiteral(sibling_list_id, value, std::make_shared<DB::DataTypeDecimal32>(precision, scale));
            }
            else if (precision <= DB::DecimalUtils::max_precision<DB::Decimal64>)
            {
                value = DB::DecimalField<DB::Decimal64>(value_ls, scale);
                state->addLiteral(sibling_list_id, value, std::make_shared<DB::DataTypeDecimal64>(precision, scale));
            }
            else if (precision <= DB::DecimalUtils::max_precision<DB::Decimal128>)
            {
                Int128 combined_value = (static_cast<DB::Int128>(value_ls) << 64) | value_ms;
                value = DB::DecimalField<DB::Decimal128>(combined_value, scale);
                state->addLiteral(sibling_list_id, value, std::make_shared<DB::DataTypeDecimal128>(precision, scale));
            }

            LOG_TEST(state->logger(), "List id: {}, type: Decimal", sibling_list_id);
        });
    }

    static void visitDateLiteral(void * data, uintptr_t sibling_list_id, int32_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: Date", sibling_list_id);

            const ExtendedDayNum daynum{value};
            state->addLiteral(sibling_list_id, value, std::make_shared<DB::DataTypeDate32>());
        });
    }

    static void visitTimestampLiteral(void * data, uintptr_t sibling_list_id, int64_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: Timestamp", sibling_list_id);

            const auto datetime_value = DB::DecimalField<DB::Decimal64>(value, 6);
            state->addLiteral(sibling_list_id, datetime_value, std::make_shared<DB::DataTypeDateTime64>(6));
        });
    }

    static void visitTimestampNtzLiteral(void * data, uintptr_t sibling_list_id, int64_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: TimestampNtz", sibling_list_id);

            const auto datetime_value = DB::DecimalField<DB::Decimal64>(value, 6);
            state->addLiteral(sibling_list_id, datetime_value, std::make_shared<DB::DataTypeDateTime64>(6));
        });
    }

    static void visitBinaryLiteral(
        void * data, uintptr_t sibling_list_id, const uint8_t * buffer, uintptr_t len)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: Binary", sibling_list_id);

            std::string value(reinterpret_cast<const char *>(buffer), len);
            state->addLiteral(sibling_list_id, value, std::make_shared<DB::DataTypeFixedString>(len));
        });
    }

    static void visitNullLiteral(void * data, uintptr_t sibling_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, type: Null", sibling_list_id);
            state->addLiteral(
                sibling_list_id,
                DB::Null(),
                std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeNothing>()));
        });
    }

    static void visitArrayLiteral(void * data, uintptr_t sibling_list_id, uintptr_t child_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->logger(), "List id: {}, child list id: {}, type: Array", sibling_list_id, child_list_id);

            auto [values, types] = state->extractLiteralList<DB::Array>(child_list_id);
            state->addLiteral(
                sibling_list_id,
                std::move(values),
                std::make_shared<DB::DataTypeArray>(types[0]));
        });
    }

    static void visitStructLiteral(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_field_list_id,
        uintptr_t child_value_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(
                state->logger(),
                "List id: {}, child field list id: {}, child value list id: {}, type: Struct",
                sibling_list_id, child_field_list_id, child_value_list_id);

            auto [values, types] = state->extractLiteralList<DB::Tuple>(child_value_list_id);
            state->addLiteral(sibling_list_id, values, std::make_shared<DB::DataTypeTuple>(types));
        });
    }

    static void visitMapLiteral(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t key_list_id,
        uintptr_t value_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(
                state->logger(),
                "List id: {}, key list id: {}, value list id: {}, type: Map",
                sibling_list_id, key_list_id, value_list_id);

            auto [keys, key_types] = state->extractLiteralList<DB::Tuple>(key_list_id);
            chassert(keys.size() == key_types.size());

            auto [values, value_types] = state->extractLiteralList<DB::Tuple>(value_list_id);
            chassert(values.size() == value_types.size());

            if (keys.empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot have empty keys");

            if (keys.size() != values.size())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Keys size does not equal values size. Keys: {}, values: {}",
                    keys.size(), values.size());
            }

            DB::Map map;
            map.reserve(keys.size());
            for (size_t i = 0; i < keys.size(); ++i)
            {
                map.emplace_back(DB::Tuple({keys[i], values[i]}));
            }
            state->addLiteral(sibling_list_id, map, std::make_shared<DB::DataTypeMap>(key_types[0], value_types[0]));
        });
    }
};

std::vector<DB::Field> getConstValuesFromExpression(const DB::Names & columns, const DB::ActionsDAG & dag)
{
    auto nodes = dag.findInOutputs(columns);
    std::vector<DB::Field> values;
    for (const auto & node : nodes)
    {
        if (node->type != DB::ActionsDAG::ActionType::COLUMN
            || !DB::isColumnConst(*node->column))
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Not a constant column: {} (column type: {})",
                magic_enum::enum_name(node->type), node->column->getDataType());
        }

        DB::Field value;
        node->column->get(0, value);
        values.push_back(std::move(value));
    }
    return values;
}

std::shared_ptr<DB::ActionsDAG> visitScanCallbackExpression(
    const ffi::Expression * expression,
    const DB::NamesAndTypesList & expression_schema)
{
    ExpressionVisitorData data(expression_schema);
    ExpressionVisitor::visit(expression, data);
    return data.getScanCallbackExpressionResult();
}

std::shared_ptr<DB::ActionsDAG> visitExpression(
    ffi::SharedExpression * expression,
    const DB::NamesAndTypesList & expression_schema)
{
    ExpressionVisitorData data(expression_schema);
    ExpressionVisitor::visit(expression, data);
    return data.getScanCallbackExpressionResult();
}

}

#endif
