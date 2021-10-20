#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Core/Block.h>

namespace DB
{

class HashJoin;
class StorageJoin;
using StorageJoinPtr = std::shared_ptr<StorageJoin>;

template <bool or_null>
class ExecutableFunctionJoinGet final : public IExecutableFunction, WithContext
{
public:
    ExecutableFunctionJoinGet(ContextPtr context_,
                              TableLockHolder table_lock_,
                              StorageJoinPtr storage_join_,
                              const DB::Block & result_columns_)
        : WithContext(context_)
        , table_lock(std::move(table_lock_))
        , storage_join(std::move(storage_join_))
        , result_columns(result_columns_)
    {}

    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    String getName() const override { return name; }

private:
    TableLockHolder table_lock;
    StorageJoinPtr storage_join;
    DB::Block result_columns;
};

template <bool or_null>
class FunctionJoinGet final : public IFunctionBase, WithContext
{
public:
    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";

    FunctionJoinGet(ContextPtr context_,
                    TableLockHolder table_lock_,
                    StorageJoinPtr storage_join_, String attr_name_,
                    DataTypes argument_types_, DataTypePtr return_type_)
        : WithContext(context_)
        , table_lock(std::move(table_lock_))
        , storage_join(storage_join_)
        , attr_name(std::move(attr_name_))
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
    {
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override;

private:
    TableLockHolder table_lock;
    StorageJoinPtr storage_join;
    const String attr_name;
    DataTypes argument_types;
    DataTypePtr return_type;
};

template <bool or_null>
class JoinGetOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";
    static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<JoinGetOverloadResolver>(context_); }

    explicit JoinGetOverloadResolver(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return {}; } // Not used

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }
};

}
