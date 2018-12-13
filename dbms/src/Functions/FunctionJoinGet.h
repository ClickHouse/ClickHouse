#include <Functions/IFunction.h>

namespace DB
{
class Context;
class Join;
using JoinPtr = std::shared_ptr<Join>;

class FunctionJoinGet final : public IFunction, public std::enable_shared_from_this<FunctionJoinGet>
{
public:
    static constexpr auto name = "joinGet";

    FunctionJoinGet(JoinPtr join, const String & attr_name) : join(std::move(join)), attr_name(attr_name) {}

    String getName() const override { return name; }
    String getSignature() const override { return {}; }

protected:
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & /*arguments*/) const override { return nullptr; }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    JoinPtr join;
    const String attr_name;
};

class FunctionBuilderJoinGet final : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "joinGet";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderJoinGet>(context); }

    FunctionBuilderJoinGet(const Context & context) : context(context) {}

    String getName() const override { return name; }
    String getSignature() const override { return {}; }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & /*arguments*/) const override { return nullptr; }

private:
    const Context & context;
};

}
