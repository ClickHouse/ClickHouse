#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>

#if USE_DELTA_KERNEL_RS
#include <Analyzer/Utils.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Functions/ComparisonNames.h>
#include <IO/WriteHelpers.h>
#include <Functions/FunctionsLogical.h>

#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

namespace DB::FailPoints
{
    extern const char delta_kernel_fail_literal_visitor[];
}

namespace DeltaLake
{

namespace
{
    /// Whether a node is a constant (literal).
    bool isConstNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::COLUMN;
    }

    /// Whether a node represents a specific column identifier, e.g. column name.
    bool isColumnNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::INPUT;
    }

    bool isFunctionNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::FUNCTION;
    }

    DB::TypeIndex getTypeIndex(const DB::ActionsDAG::Node * node)
    {
        if (!node->result_type->isNullable())
            return node->result_type->getTypeId();

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(node->result_type.get());
        return nullable->getNestedType()->getTypeId();
    }

    DB::DataTypePtr getTypeOrNestedType(const DB::ActionsDAG::Node * node)
    {
        if (!node->result_type->isNullable())
            return node->result_type;

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(node->result_type.get());
        return nullable->getNestedType();
    }
}

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception, DB::ContextPtr context)
{
    return std::make_unique<EnginePredicate>(filter, exception, context);
}

/// Contains state for EngineIterator
/// (an iterator over DB::ActionsDAG const node ptr's).
struct EngineIteratorData
{
    EngineIteratorData(
        ffi::KernelExpressionVisitorState * state_,
        const DB::ActionsDAG::NodeRawConstPtrs & nodes_,
        EnginePredicate & predicate_)
        : state(state_)
        , predicate(predicate_)
        , nodes(nodes_)
        , it(nodes.begin())
    {
    }

    void setException(std::exception_ptr exception_)
    {
        predicate.setException(exception_);
    }

    bool hasException() const { return predicate.exception != nullptr; }

    const LoggerPtr & log() const { return predicate.log; }

    const DB::ActionsDAG::Node * next()
    {
        if (it == nodes.end())
            return {};
        return *(it++);
    }

    ffi::KernelExpressionVisitorState * state;
    EnginePredicate & predicate;

private:
    const DB::ActionsDAG::NodeRawConstPtrs & nodes;
    DB::ActionsDAG::NodeRawConstPtrs::const_iterator it;
};

/// An iterator over DB::ActionsDAG const node ptr's.
/// Applies corresponding delta-kernel visitors to each node.
class  EngineIterator : public ffi::EngineIterator
{
public:
    static constexpr uint64_t VISITOR_FAILED_OR_UNSUPPORTED = ~0;

    explicit EngineIterator(EngineIteratorData & data_) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        data = &data_;
        get_next = &getNext;
    }

private:
    /// Name of the `Unknown` predicate handed to delta-kernel for a sub-expression we could not
    /// translate. It must be a fixed ASCII literal: visit_predicate_unknown validates the name as
    /// UTF-8 and reports failure for anything else, while a DAG node's result_name may hold
    /// arbitrary bytes (e.g. a binary string literal appearing in the filter).
    static constexpr std::string_view UNTRANSLATED_PREDICATE_NAME = "clickhouse_untranslated";

    /// Represent "this node could not be translated" to delta-kernel.
    ///
    /// Returning nullptr instead would mean *the iterator is exhausted*, which truncates the
    /// enclosing junction rather than dropping one child. That is only harmless in monotone
    /// position: an empty conjunction normalizes to TRUE, so under NOT it becomes FALSE and every
    /// data file gets skipped; a partially consumed conjunction under NOT yields a predicate
    /// narrower than the truth. An explicit Unknown is "cannot decide" in the kernel's
    /// three-valued logic and never skips a file on its own account, in any polarity.
    static uintptr_t visitUntranslated(EngineIteratorData & iterator_data)
    {
        const std::string name{UNTRANSLATED_PREDICATE_NAME};
        auto unknown = ffi::visit_predicate_unknown(iterator_data.state, KernelUtils::toDeltaString(name));
        if (!unknown)
        {
            /// Unreachable with a compile-time ASCII name unless an invariant broke: 0 is the
            /// kernel's reserved "no id" sentinel, and handing it on would reintroduce the very
            /// truncation this function exists to prevent.
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "delta-kernel rejected the `{}` predicate name", name);
        }
        return unknown;
    }

    static const void * getNext(void * data_)
    {
        auto * iterator_data = static_cast<EngineIteratorData *>(data_);
        try
        {
            if (iterator_data->hasException())
            {
                LOG_TEST(iterator_data->log(), "Exception during processing");
                return nullptr;
            }

            const auto * node = iterator_data->next();
            if (!node)
            {
                /// Real exhaustion, which is what nullptr means to the kernel.
                LOG_TEST(iterator_data->log(), "Iterator finished");
                return nullptr;
            }

            LOG_TEST(
                iterator_data->log(),
                "Node name: {}, node type: {}, column type: {}",
                node->result_name,
                node->type,
                node->column ? toString(node->column->getDataType()) : "None");

            auto result = getNextImpl(*iterator_data, node);
            if (result && result != VISITOR_FAILED_OR_UNSUPPORTED)
            {
                return reinterpret_cast<const void *>(result);
            }

            LOG_TEST(iterator_data->log(), "Node could not be translated, visiting it as unknown");
        }
        catch (...)
        {
            iterator_data->setException(std::current_exception());
        }

        /// Reached when the node was not translated, either because the visitor reported
        /// failure or because it threw (the exception stays recorded on the shared predicate).
        /// This function is invoked from Rust through an `extern "C"` pointer, so an exception
        /// must not leave it.
        try
        {
            return reinterpret_cast<const void *>(visitUntranslated(*iterator_data));
        }
        catch (...)
        {
            iterator_data->setException(std::current_exception());
            return nullptr;
        }
    }

    static uintptr_t getNextImpl(EngineIteratorData & iterator_data, const DB::ActionsDAG::Node * node);
};

uintptr_t EnginePredicate::visitPredicate(void * data, ffi::KernelExpressionVisitorState * state)
{
    auto * predicate = static_cast<EnginePredicate *>(data);
    EngineIteratorData iterator_data(state, predicate->filter.getOutputs(), *predicate);
    EngineIterator engine_iterator(iterator_data);
    auto result = ffi::visit_predicate_and(state, &engine_iterator);

    LOG_TEST(iterator_data.log(), "visitPredicate finished (exception: {})", predicate->hasException());
    return result;
}

static uintptr_t visitLiteralValue(
    const DB::Field & value,
    DB::TypeIndex type_index,
    DB::DataTypePtr data_type,
    ffi::KernelExpressionVisitorState * state)
{
    LOG_TEST(getLogger("EnginePredicate"), "Type index: {}, data type: {}", type_index, data_type->getName());

    fiu_do_on(DB::FailPoints::delta_kernel_fail_literal_visitor,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault for visitLiteralValue");
    });

    switch (type_index)
    {
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
        {
            const auto & value_str = value.safeGet<String>();
            auto value_delta_str = KernelUtils::toDeltaString(value_str);
            return KernelUtils::unwrapResult(
                ffi::visit_expression_literal_string(
                    state,
                    value_delta_str,
                    &KernelUtils::allocateError), "visit_expression_literal_string");
        }
        case DB::TypeIndex::Int8:
        {
            auto result = value.safeGet<Int8>();
            return ffi::visit_expression_literal_byte(state, static_cast<int8_t>(result));
        }
        case DB::TypeIndex::UInt8:
        {
            if (isBool(data_type))
            {
                bool result = value.safeGet<UInt8>();
                return ffi::visit_expression_literal_bool(state, result); /// Accepts bool
            }
            else
            {
                auto result = value.safeGet<Int16>();
                return ffi::visit_expression_literal_short(state, static_cast<int16_t>(result));
            }
        }
        case DB::TypeIndex::Int16:
        {
            auto result = value.safeGet<Int16>();
            return ffi::visit_expression_literal_short(state, static_cast<int16_t>(result));
        }
        case DB::TypeIndex::UInt16:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_int(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::Int32:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_int(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::UInt32:
        {
            auto result = value.safeGet<Int64>();
            return ffi::visit_expression_literal_long(state, result); /// Accepts int64
        }
        case DB::TypeIndex::Int64:
        {
            auto result = value.safeGet<Int64>();
            return ffi::visit_expression_literal_long(state, result); /// Accepts int64
        }
        case DB::TypeIndex::Date:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_date(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::Date32:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_date(state, static_cast<int32_t>(result));
        }
        default:
        {
            return EngineIterator::VISITOR_FAILED_OR_UNSUPPORTED;
        }
    }
}

uintptr_t EngineIterator::getNextImpl(EngineIteratorData & iterator_data, const DB::ActionsDAG::Node * node)
{
    if (iterator_data.hasException())
    {
        LOG_TEST(iterator_data.log(), "Exception during processing, returning from getNextImpl");
        return VISITOR_FAILED_OR_UNSUPPORTED;
    }

    switch (node->type)
    {
        case DB::ActionsDAG::ActionType::FUNCTION:
        {
            const auto func_name = node->function_base->getName();
            LOG_TEST(iterator_data.log(), "Function: {}", func_name);

            if (func_name == DB::NameAnd::name)
            {
                EngineIteratorData current_iterator_data(
                        iterator_data.state,
                        node->children,
                        iterator_data.predicate);

                EngineIterator current_engine_iterator(current_iterator_data);

                return ffi::visit_predicate_and(iterator_data.state, &current_engine_iterator);
            }
            else if (func_name == DB::NameNot::name)
            {
                if (node->children.size() != 1)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Expected function `{}` to have 1 child node, got {}",
                        func_name, node->children.size());
                }

                if (isColumnNode(node->children[0]))
                {
                    const auto column_name = KernelUtils::toDeltaString(node->children[0]->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                     column_name,
                                                     &KernelUtils::allocateError), "visit_expression_column");
                    return ffi::visit_predicate_not(iterator_data.state, column);
                }

                if (isFunctionNode(node->children[0]))
                {
                    EngineIteratorData current_iterator_data(
                            iterator_data.state,
                            node->children,
                            iterator_data.predicate);

                    EngineIterator current_engine_iterator(current_iterator_data);
                    auto column = ffi::visit_predicate_and(iterator_data.state, &current_engine_iterator);
                    return ffi::visit_predicate_not(iterator_data.state, column);
                }
            }
            else if (func_name == DB::NameEquals::name
                     || func_name == DB::NameNotEquals::name
                     || func_name == DB::NameGreater::name
                     || func_name == DB::NameGreaterOrEquals::name
                     || func_name == DB::NameLess::name
                     || func_name == DB::NameLessOrEquals::name)
            {
                if (node->children.size() != 2)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Expected function `{}` to have 2 child nodes, got {}",
                        func_name, node->children.size());
                }

                auto print_node_info = [&](const DB::ActionsDAG::Node * node_)
                {
                    LOG_TEST(getLogger("test"),
                             "Left node type: {}, result name: {}, result type: {}, "
                             "is constant: {}, column: {}, column type: {}",
                             node_->type,
                             node_->result_name,
                             node_->result_type,
                             node_->is_deterministic_constant,
                             bool(node_->column),
                             node_->column ? DB::toString(node_->column->getDataType()) : "None");
                };
                print_node_info(node->children[0]);
                print_node_info(node->children[1]);

                const DB::ActionsDAG::Node * column_node = nullptr;
                const DB::ActionsDAG::Node * literal_node = nullptr;
                if (isConstNode(node->children[0]) && isColumnNode(node->children[1]))
                {
                    literal_node = node->children[0];
                    column_node = node->children[1];
                }
                else if (isConstNode(node->children[1]) && isColumnNode(node->children[0]))
                {
                    literal_node = node->children[1];
                    column_node = node->children[0];
                }

                if (literal_node && column_node)
                {
                    /// If literal node has a different type from column's,
                    /// cast it to column's type.
                    if (!column_node->result_type->equals(*literal_node->result_type))
                    {
                        auto column_name = column_node->result_type->getName();
                        auto column_type = std::make_shared<DB::DataTypeString>();
                        auto column = assert_cast<const DB::ColumnConst &>(*column_type->createColumnConst(0, column_name)).getPtr();

                        /// TODO: get rid of const_cast.
                        DB::ActionsDAG & dag = const_cast<DB::ActionsDAG &>(iterator_data.predicate.getFilterDAG());

                        const auto * right_arg = &dag.addColumn(std::move(column), std::move(column_type), std::move(column_name));
                        const auto * left_arg = literal_node;

                        DB::CastDiagnostic diagnostic = {literal_node->result_name, column_node->result_name};
                        DB::ColumnWithTypeAndName left_column{nullptr, literal_node->result_type, {}};
                        auto func_base_cast = DB::createInternalCast(
                            /* from */std::move(left_column),
                            /* to */column_node->result_type,
                            DB::CastType::nonAccurate,
                            std::move(diagnostic),
                            iterator_data.predicate.getContext());

                        DB::ActionsDAG::NodeRawConstPtrs children = { left_arg, right_arg };
                        literal_node = &dag.addFunction(func_base_cast, std::move(children), {});

                        print_node_info(literal_node);
                    }

                    const auto column_name = KernelUtils::toDeltaString(column_node->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                    column_name,
                                                    &KernelUtils::allocateError), "visit_expression_column");

                    const auto comparison_type_index = getTypeIndex(column_node);

                    DB::Field value = literal_node->column->getField();

                    uintptr_t constant = visitLiteralValue(
                        value,
                        comparison_type_index,
                        getTypeOrNestedType(column_node),
                        iterator_data.state);

                    if (!constant || constant == VISITOR_FAILED_OR_UNSUPPORTED)
                    {
                        LOG_TEST(iterator_data.log(), "Unsupported literal type: {}", comparison_type_index);
                        return VISITOR_FAILED_OR_UNSUPPORTED;
                    }

                    if (func_name == DB::NameEquals::name)
                        return ffi::visit_predicate_eq(iterator_data.state, column, constant);
                    if (func_name == DB::NameNotEquals::name)
                        return ffi::visit_predicate_ne(iterator_data.state, column, constant);
                    if (func_name == DB::NameGreater::name)
                        return ffi::visit_predicate_gt(iterator_data.state, column, constant);
                    if (func_name == DB::NameGreaterOrEquals::name)
                        return ffi::visit_predicate_ge(iterator_data.state, column, constant);
                    if (func_name == DB::NameLess::name)
                        return ffi::visit_predicate_lt(iterator_data.state, column, constant);
                    if (func_name == DB::NameLessOrEquals::name)
                        return ffi::visit_predicate_le(iterator_data.state, column, constant);
                }
            }

            break;
        }
        default:
        {
            break;
        }
    }
    return VISITOR_FAILED_OR_UNSUPPORTED;
}

}

#endif
