#include "EnginePredicate.h"

#include <Common/logger_useful.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>

#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <delta_kernel_ffi.hpp>

namespace DeltaLake
{

namespace
{
    bool isConstNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::COLUMN;
    }

    bool isColumnNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::INPUT;
    }
}

/// A predicate which allows to implement internal delta-kernel-rs filtering
/// based on statistics and partitions pruning.
class EnginePredicate : public ffi::EnginePredicate
{
    friend struct EngineIteratorData;
public:
    explicit EnginePredicate(
        const DB::ActionsDAG & filter_,
        std::exception_ptr & exception_)
        : filter(filter_)
        , exception(exception_)
    {
        predicate = this;
        visitor = &visitPredicate;
    }

    void setException(std::exception_ptr exception_)
    {
        DB::tryLogException(exception_, log);
        if (!exception)
            exception = exception_;
    }

    const LoggerPtr log = getLogger("EnginePredicate");

private:
    /// Predicate expression.
    const DB::ActionsDAG & filter;
    /// Exception which will be set during EnginePredicate execution.
    /// Exceptions cannot be rethrown as it will cause
    /// panic from rust and server terminate.
    std::exception_ptr & exception;

    static uintptr_t visitPredicate(void * data, ffi::KernelExpressionVisitorState * state);
};

std::unique_ptr<ffi::EnginePredicate> getEnginePredicate(const DB::ActionsDAG & filter, std::exception_ptr & exception)
{
    return std::make_unique<EnginePredicate>(filter, exception);
}

/// Contains state for EngineIterator.
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
class  EngineIterator : public ffi::EngineIterator
{
public:
    explicit EngineIterator(EngineIteratorData & data_)
    {
        data = &data_;
        get_next = &getNext;
    }

private:
    static const void * getNext(void * data_)
    {
        auto * iterator_data = static_cast<EngineIteratorData *>(data_);
        LOG_TEST(iterator_data->log(), "Next");
        try
        {
            auto result = getNextImpl(*iterator_data);
            if (result)
                return reinterpret_cast<const void *>(result);
        }
        catch (...)
        {
            iterator_data->setException(std::current_exception());
        }
        return {};
    }

    static uintptr_t getNextImpl(EngineIteratorData & iterator_data);
    static uintptr_t visitLiteralValue(
        const DB::Field & value,
        DB::TypeIndex type_index,
        ffi::KernelExpressionVisitorState * state);
};

uintptr_t EnginePredicate::visitPredicate(void * data, ffi::KernelExpressionVisitorState * state)
{
    auto * predicate = static_cast<EnginePredicate *>(data);
    EngineIteratorData iterator_data(state, predicate->filter.getOutputs(), *predicate);
    EngineIterator engine_iterator(iterator_data);
    return ffi::visit_predicate_and(state, &engine_iterator);
}

uintptr_t EngineIterator::visitLiteralValue(
    const DB::Field & value,
    DB::TypeIndex type_index,
    ffi::KernelExpressionVisitorState * state)
{
    switch (type_index)
    {
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
        {
            static constexpr auto test = "test2";
            //auto value_str = KernelUtils::toDeltaString(value.safeGet<String>());
            auto value_str = KernelUtils::toDeltaString(test);
            return KernelUtils::unwrapResult(
                ffi::visit_expression_literal_string(
                    state,
                    value_str,
                    &KernelUtils::allocateError), "visit_expression_literal_string");
        }
        case DB::TypeIndex::Int8:
        {
            return ffi::visit_expression_literal_byte(state, value.safeGet<Int8>());
        }
        case DB::TypeIndex::UInt8:
        {
            return ffi::visit_expression_literal_byte(state, value.safeGet<UInt8>());
        }
        case DB::TypeIndex::Int16:
        {
            return ffi::visit_expression_literal_short(state, value.safeGet<Int16>());
        }
        case DB::TypeIndex::UInt16:
        {
            return ffi::visit_expression_literal_short(state, value.safeGet<UInt16>());
        }
        case DB::TypeIndex::Int32:
        {
            return ffi::visit_expression_literal_long(state, value.safeGet<Int32>());
        }
        case DB::TypeIndex::UInt32:
        {
            return ffi::visit_expression_literal_long(state, value.safeGet<UInt32>());
        }
        case DB::TypeIndex::Int64:
        {
            return ffi::visit_expression_literal_long(state, value.safeGet<Int64>());
        }
        //case DB::TypeIndex::UInt64:
        //{
        //    return ffi::visit_expression_literal_long(state, value.safeGet<UInt64>());
        //}
        default:
        {
            return {};
        }
    }
}

uintptr_t EngineIterator::getNextImpl(EngineIteratorData & iterator_data)
{
    if (iterator_data.hasException())
        return {};

    const auto * node = iterator_data.next();
    if (!node)
    {
        LOG_TEST(iterator_data.log(), "Iterator finished");
        return {};
    }

    LOG_TEST(iterator_data.log(), "Node name: {}, node type: {}", node->result_name, node->type);

    switch (node->type)
    {
        case DB::ActionsDAG::ActionType::FUNCTION:
        {
            const auto func_name = node->function_base->getName();
            LOG_TEST(iterator_data.log(), "Function: {}", func_name);

            if (func_name == DB::NameAnd::name)
            {
                EngineIteratorData current_iterator_data(iterator_data.state, node->children, iterator_data.predicate);
                EngineIterator current_engine_iterator(current_iterator_data);

                return ffi::visit_predicate_and(iterator_data.state, &current_engine_iterator);
            }
            else if (func_name == DB::NameEquals::name)
            {
                if (node->children.size() != 2)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Expected function `equals` to have 2 child nodes, got {}",
                        node->children.size());
                }

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
                    const auto column_name = KernelUtils::toDeltaString(column_node->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                    column_name,
                                                    &KernelUtils::allocateError), "visit_expression_column");

                    DB::Field value;
                    literal_node->column->get(0, value);
                    DB::TypeIndex type_index = literal_node->result_type->isNullable()
                        ? assert_cast<const DB::DataTypeNullable *>(literal_node->result_type.get())->getNestedType()->getTypeId()
                        : literal_node->result_type->getTypeId();

                    uintptr_t constant = visitLiteralValue(value, type_index, iterator_data.state);
                    if (!constant)
                    {
                        LOG_TEST(iterator_data.log(), "Unsupported literal type: {}", type_index);
                        return {};
                    }

                    return ffi::visit_predicate_eq(iterator_data.state, column, constant);
                }
            }

            break;
        }
        default:
        {
            break;
        }
    }
    return {};
}

}
