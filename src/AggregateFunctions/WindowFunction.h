#pragma once
#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/WindowDescription.h>
#include <Common/AlignedBuffer.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
class WindowTransform;


// Interface for true window functions. It's not much of an interface, they just
// accept the guts of WindowTransform and do 'something'. Given a small number of
// true window functions, and the fact that the WindowTransform internals are
// pretty much well-defined in domain terms (e.g. frame boundaries), this is
// somewhat acceptable.
class IWindowFunction
{
public:
    virtual ~IWindowFunction() = default;

    // Must insert the result for current_row.
    virtual void windowInsertResultInto(const WindowTransform * transform, size_t function_index) const = 0;

    virtual std::optional<WindowFrame> getDefaultFrame() const { return {}; }

    virtual ColumnPtr castColumn(const Columns &, const std::vector<size_t> &) { return nullptr; }

    /// Is the frame type supported by this function.
    virtual bool checkWindowFrameType(const WindowTransform * /*transform*/) const { return true; }
};

// Runtime data for computing one window function.
struct WindowFunctionWorkspace
{
    AggregateFunctionPtr aggregate_function;

    // Cached value of aggregate function isState virtual method
    bool is_aggregate_function_state = false;

    // This field is set for pure window functions. When set, we ignore the
    // window_function.aggregate_function, and work through this interface
    // instead.
    IWindowFunction * window_function_impl = nullptr;

    std::vector<size_t> argument_column_indices;

    // Will not be initialized for a pure window function.
    mutable AlignedBuffer aggregate_function_state;

    // Argument columns. Be careful, this is a per-block cache.
    std::vector<const IColumn *> argument_columns;
    UInt64 cached_block_number = std::numeric_limits<UInt64>::max();
};

// A basic implementation for a true window function. It pretends to be an
// aggregate function, but refuses to work as such.
struct WindowFunction : public IAggregateFunctionHelper<WindowFunction>, public IWindowFunction
{
    std::string name;

    WindowFunction(
        const std::string & name_, const DataTypes & argument_types_, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionHelper<WindowFunction>(argument_types_, parameters_, result_type_), name(name_)
    {
    }

    bool isOnlyWindowFunction() const override { return true; }

    [[noreturn]] void fail() const
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The function '{}' can only be used as a window function, not as an aggregate function", getName());
    }

    String getName() const override { return name; }
    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override { fail(); }
    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override { fail(); }
    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer &, std::optional<size_t>) const override { fail(); }
    void deserialize(AggregateDataPtr __restrict, ReadBuffer &, std::optional<size_t>, Arena *) const override { fail(); }
    void insertResultInto(AggregateDataPtr __restrict, IColumn &, Arena *) const override { fail(); }
};

struct StatelessWindowFunction : public WindowFunction
{
    StatelessWindowFunction(
        const std::string & name_, const DataTypes & argument_types_, const Array & parameters_, const DataTypePtr & result_type_)
        : WindowFunction(name_, argument_types_, parameters_, result_type_)
    {
    }

    size_t sizeOfData() const override { return 0; }
    size_t alignOfData() const override { return 1; }

    void create(AggregateDataPtr __restrict) const override { }
    void destroy(AggregateDataPtr __restrict) const noexcept override { }
    bool hasTrivialDestructor() const override { return true; }
};

template <typename State>
struct StatefulWindowFunction : public WindowFunction
{
    StatefulWindowFunction(
        const std::string & name_, const DataTypes & argument_types_, const Array & parameters_, const DataTypePtr & result_type_)
        : WindowFunction(name_, argument_types_, parameters_, result_type_)
    {
    }

    size_t sizeOfData() const override { return sizeof(State); }
    size_t alignOfData() const override { return alignof(State); }

    void create(AggregateDataPtr __restrict place) const override { new (place) State(); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { reinterpret_cast<State *>(place)->~State(); }

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<State>; }

    State & getState(const WindowFunctionWorkspace & workspace) const
    {
        return *reinterpret_cast<State *>(workspace.aggregate_function_state.data());
    }
};

}
