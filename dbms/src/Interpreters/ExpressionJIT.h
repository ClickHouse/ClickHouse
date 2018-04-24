#pragma once

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>

#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class LLVMContext
{
    struct Data;
    std::shared_ptr<Data> shared;

public:
    LLVMContext();

    void finalize();

    bool isCompilable(const IFunctionBase& function) const;

    Data * operator->() const {
        return shared.get();
    }
};

// second array is of `char` because `LLVMPreparedFunction::executeImpl` can't use a `std::vector<bool>` for this
using LLVMCompiledFunction = void(const void ** inputs, const char * is_constant, void * output, size_t block_size);

class LLVMPreparedFunction : public PreparedFunctionImpl
{
    std::shared_ptr<const IFunctionBase> parent;
    LLVMContext context;
    LLVMCompiledFunction * function;

public:
    LLVMPreparedFunction(LLVMContext context, std::shared_ptr<const IFunctionBase> parent);

    String getName() const override { return parent->getName(); }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t block_size = 0;
        std::vector<const void *> columns(arguments.size());
        std::vector<char> is_const(arguments.size());
        for (size_t i = 0; i < arguments.size(); i++)
        {
            auto * column = block.getByPosition(arguments[i]).column.get();
            if (column->size())
                // assume the column is a `ColumnVector<T>`. there's probably no good way to actually
                // check that at runtime, so let's just hope it's always true for columns containing types
                // for which `LLVMSharedData::toNativeType` returns non-null.
                columns[i] = column->getDataAt(0).data;
            is_const[i] = column->isColumnConst();
            block_size = column->size();
        }
        auto col_res = createColumn(parent->getReturnType(), block_size);
        if (block_size)
            function(columns.data(), is_const.data(), const_cast<char *>(col_res->getDataAt(0).data), block_size);
        block.getByPosition(result).column = std::move(col_res);
    };

private:
    static IColumn::Ptr createColumn(const DataTypePtr & type, size_t size)
    {
        if (type->equals(DataTypeInt8{}))
            return ColumnVector<Int8>::create(size);
        if (type->equals(DataTypeInt16{}))
            return ColumnVector<Int16>::create(size);
        if (type->equals(DataTypeInt32{}))
            return ColumnVector<Int32>::create(size);
        if (type->equals(DataTypeInt64{}))
            return ColumnVector<Int64>::create(size);
        if (type->equals(DataTypeUInt8{}))
            return ColumnVector<UInt8>::create(size);
        if (type->equals(DataTypeUInt16{}))
            return ColumnVector<UInt16>::create(size);
        if (type->equals(DataTypeUInt32{}))
            return ColumnVector<UInt32>::create(size);
        if (type->equals(DataTypeUInt64{}))
            return ColumnVector<UInt64>::create(size);
        if (type->equals(DataTypeFloat32{}))
            return ColumnVector<Float32>::create(size);
        if (type->equals(DataTypeFloat64{}))
            return ColumnVector<Float64>::create(size);
        throw Exception("LLVMPreparedFunction::createColumn received an unsupported data type; check "
                        "that the list is consistent with LLVMContext::Data::toNativeType", ErrorCodes::LOGICAL_ERROR);
    }
};

class LLVMFunction : public std::enable_shared_from_this<LLVMFunction>, public IFunctionBase
{
    ExpressionActions::Actions actions; // all of them must have type APPLY_FUNCTION
    Names arg_names;
    DataTypes arg_types;
    LLVMContext context;

public:
    LLVMFunction(ExpressionActions::Actions actions, LLVMContext context);

    String getName() const override { return actions.back().result_name; }

    const Names & getArgumentNames() const { return arg_names; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getReturnType() const override { return actions.back().function->getReturnType(); }

    PreparedFunctionPtr prepare(const Block &) const override { return std::make_shared<LLVMPreparedFunction>(context, shared_from_this()); }

    bool isDeterministic() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministic())
                return false;
        return true;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministicInScopeOfQuery())
                return false;
        return true;
    }

    // TODO: these methods require reconstructing the call tree:
    // bool isSuitableForConstantFolding() const;
    // bool isInjective(const Block & sample_block);
    // bool hasInformationAboutMonotonicity() const;
    // Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const;
};

}
