#pragma once
#include <Functions/IFunctionImpl.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Core/Block.h>

namespace DB
{

class Context;
class HashJoin;
using HashJoinPtr = std::shared_ptr<HashJoin>;

template <bool or_null>
class ExecutableFunctionJoinGet final : public IExecutableFunctionImpl
{
public:
    ExecutableFunctionJoinGet(HashJoinPtr join_, const DB::Block & result_columns_)
        : join(std::move(join_)), result_columns(result_columns_) {}

    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    String getName() const override { return name; }

private:
    HashJoinPtr join;
    DB::Block result_columns;
};

template <bool or_null>
class FunctionJoinGet final : public IFunctionBaseImpl
{
public:
    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";

    FunctionJoinGet(TableLockHolder table_lock_, StoragePtr storage_join_,
                    HashJoinPtr join_, String attr_name_,
                    DataTypes argument_types_, DataTypePtr return_type_)
        : table_lock(std::move(table_lock_))
        , storage_join(std::move(storage_join_))
        , join(std::move(join_))
        , attr_name(std::move(attr_name_))
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
    {
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override;

private:
    TableLockHolder table_lock;
    StoragePtr storage_join;
    HashJoinPtr join;
    const String attr_name;
    DataTypes argument_types;
    DataTypePtr return_type;
};

template <bool or_null>
class JoinGetOverloadResolver final : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = or_null ? "joinGetOrNull" : "joinGet";
    static FunctionOverloadResolverImplPtr create(const Context & context) { return std::make_unique<JoinGetOverloadResolver>(context); }

    explicit JoinGetOverloadResolver(const Context & context_) : context(context_) {}

    String getName() const override { return name; }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnType(const ColumnsWithTypeAndName &) const override { return {}; } // Not used

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

private:
    const Context & context;
};

}
