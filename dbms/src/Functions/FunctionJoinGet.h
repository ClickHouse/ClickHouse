#include <Functions/IFunction.h>
#include <Storages/TableStructureLockHolder.h>

namespace DB
{
class Context;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
class Join;
using JoinPtr = std::shared_ptr<Join>;

class FunctionJoinGet final : public IFunction
{
public:
    static constexpr auto name = "joinGet";

    FunctionJoinGet(
        TableStructureReadLockHolder table_lock, StoragePtr storage_join, JoinPtr join, const String & attr_name, DataTypePtr return_type)
        : table_lock(std::move(table_lock))
        , storage_join(std::move(storage_join))
        , join(std::move(join))
        , attr_name(attr_name)
        , return_type(std::move(return_type))
    {
    }

    String getName() const override { return name; }

protected:
    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return return_type; }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

private:
    TableStructureReadLockHolder table_lock;
    StoragePtr storage_join;
    JoinPtr join;
    const String attr_name;
    DataTypePtr return_type;
};

class FunctionBuilderJoinGet final : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "joinGet";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderJoinGet>(context); }

    FunctionBuilderJoinGet(const Context & context) : context(context) {}

    String getName() const override { return name; }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

private:
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

private:
    const Context & context;
};

}
