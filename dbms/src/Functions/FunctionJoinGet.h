#include <Functions/IFunctionImpl.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableStructureLockHolder.h>

namespace DB
{

class Context;
class Join;
using HashJoinPtr = std::shared_ptr<Join>;

class ExecutableFunctionJoinGet final : public IExecutableFunctionImpl
{
public:
    ExecutableFunctionJoinGet(HashJoinPtr join_, String attr_name_)
        : join(std::move(join_)), attr_name(std::move(attr_name_)) {}

    static constexpr auto name = "joinGet";

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

    String getName() const override { return name; }

private:
    HashJoinPtr join;
    const String attr_name;
};

class FunctionJoinGet final : public IFunctionBaseImpl
{
public:
    static constexpr auto name = "joinGet";

    FunctionJoinGet(TableStructureReadLockHolder table_lock_, StoragePtr storage_join_,
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
    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const Block & sample_block, const ColumnNumbers & arguments, size_t result) const override;

private:
    TableStructureReadLockHolder table_lock;
    StoragePtr storage_join;
    HashJoinPtr join;
    const String attr_name;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class JoinGetOverloadResolver final : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = "joinGet";
    static FunctionOverloadResolverImplPtr create(const Context & context) { return std::make_unique<JoinGetOverloadResolver>(context); }

    explicit JoinGetOverloadResolver(const Context & context_) : context(context_) {}

    String getName() const override { return name; }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override;

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

private:
    const Context & context;
};

}
