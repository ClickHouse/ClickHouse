#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnsCommon.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteHelpers.h>
#include <Functions/IFunction.h>

namespace ProfileEvents
{
    extern const Event FunctionExecute;
    extern const Event CompiledFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

ColumnFunction::ColumnFunction(size_t size, FunctionBasePtr function_, const ColumnsWithTypeAndName & columns_to_capture, bool is_short_circuit_argument_, bool is_function_compiled_)
        : size_(size), function(function_), is_short_circuit_argument(is_short_circuit_argument_), is_function_compiled(is_function_compiled_)
{
    appendArguments(columns_to_capture);
}

MutableColumnPtr ColumnFunction::cloneResized(size_t size) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cloneResized(size);

    return ColumnFunction::create(size, function, capture, is_short_circuit_argument, is_function_compiled);
}

ColumnPtr ColumnFunction::replicate(const Offsets & offsets) const
{
    if (size_ != offsets.size())
        throw Exception("Size of offsets (" + toString(offsets.size()) + ") doesn't match size of column ("
                        + toString(size_) + ")", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->replicate(offsets);

    size_t replicated_size = 0 == size_ ? 0 : offsets.back();
    return ColumnFunction::create(replicated_size, function, capture, is_short_circuit_argument, is_function_compiled);
}

ColumnPtr ColumnFunction::cut(size_t start, size_t length) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->cut(start, length);

    return ColumnFunction::create(length, function, capture, is_short_circuit_argument, is_function_compiled);
}

ColumnPtr ColumnFunction::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (size_ != filt.size())
        throw Exception("Size of filter (" + toString(filt.size()) + ") doesn't match size of column ("
                        + toString(size_) + ")", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->filter(filt, result_size_hint);

    size_t filtered_size = 0;
    if (capture.empty())
    {
        filtered_size = countBytesInFilter(filt);
    }
    else
        filtered_size = capture.front().column->size();

    return ColumnFunction::create(filtered_size, function, capture, is_short_circuit_argument, is_function_compiled);
}

void ColumnFunction::expand(const Filter & mask, bool inverted)
{
    for (auto & column : captured_columns)
    {
        column.column = column.column->cloneResized(column.column->size());
        column.column->assumeMutable()->expand(mask, inverted);
    }

    size_ = mask.size();
}

ColumnPtr ColumnFunction::permute(const Permutation & perm, size_t limit) const
{
    limit = getLimitForPermutation(size(), perm.size(), limit);

    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->permute(perm, limit);

    return ColumnFunction::create(limit, function, capture, is_short_circuit_argument, is_function_compiled);
}

ColumnPtr ColumnFunction::index(const IColumn & indexes, size_t limit) const
{
    ColumnsWithTypeAndName capture = captured_columns;
    for (auto & column : capture)
        column.column = column.column->index(indexes, limit);

    return ColumnFunction::create(limit, function, capture, is_short_circuit_argument, is_function_compiled);
}

std::vector<MutableColumnPtr> ColumnFunction::scatter(IColumn::ColumnIndex num_columns,
                                                      const IColumn::Selector & selector) const
{
    if (size_ != selector.size())
        throw Exception("Size of selector (" + toString(selector.size()) + ") doesn't match size of column ("
                        + toString(size_) + ")", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<size_t> counts;
    if (captured_columns.empty())
        counts = countColumnsSizeInSelector(num_columns, selector);

    std::vector<ColumnsWithTypeAndName> captures(num_columns, captured_columns);

    for (size_t capture = 0; capture < captured_columns.size(); ++capture)
    {
        auto parts = captured_columns[capture].column->scatter(num_columns, selector);
        for (IColumn::ColumnIndex part = 0; part < num_columns; ++part)
            captures[part][capture].column = std::move(parts[part]);
    }

    std::vector<MutableColumnPtr> columns;
    columns.reserve(num_columns);
    for (IColumn::ColumnIndex part = 0; part < num_columns; ++part)
    {
        auto & capture = captures[part];
        size_t capture_size = capture.empty() ? counts[part] : capture.front().column->size();
        columns.emplace_back(ColumnFunction::create(capture_size, function, std::move(capture), is_short_circuit_argument));
    }

    return columns;
}

size_t ColumnFunction::byteSize() const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->byteSize();

    return total_size;
}

size_t ColumnFunction::byteSizeAt(size_t n) const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->byteSizeAt(n);

    return total_size;
}

size_t ColumnFunction::allocatedBytes() const
{
    size_t total_size = 0;
    for (const auto & column : captured_columns)
        total_size += column.column->allocatedBytes();

    return total_size;
}

void ColumnFunction::appendArguments(const ColumnsWithTypeAndName & columns)
{
    auto args = function->getArgumentTypes().size();
    auto were_captured = captured_columns.size();
    auto wanna_capture = columns.size();

    if (were_captured + wanna_capture > args)
        throw Exception("Cannot capture " + toString(wanna_capture) + " columns because function " + function->getName()
                        + " has " + toString(args) + " arguments" +
                        (were_captured ? " and " + toString(were_captured) + " columns have already been captured" : "")
                        + ".", ErrorCodes::LOGICAL_ERROR);

    for (const auto & column : columns)
        appendArgument(column);
}

void ColumnFunction::appendArgument(const ColumnWithTypeAndName & column)
{
    const auto & argumnet_types = function->getArgumentTypes();

    auto index = captured_columns.size();
    if (!is_short_circuit_argument && !column.type->equals(*argumnet_types[index]))
        throw Exception("Cannot capture column " + std::to_string(argumnet_types.size()) +
                        " because it has incompatible type: got " + column.type->getName() +
                        ", but " + argumnet_types[index]->getName() + " is expected.", ErrorCodes::LOGICAL_ERROR);

    captured_columns.push_back(column);
}

DataTypePtr ColumnFunction::getResultType() const
{
    return function->getResultType();
}

ColumnWithTypeAndName ColumnFunction::reduce() const
{
    auto args = function->getArgumentTypes().size();
    auto captured = captured_columns.size();

    if (args != captured)
        throw Exception("Cannot call function " + function->getName() + " because is has " + toString(args) +
                        "arguments but " + toString(captured) + " columns were captured.", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = captured_columns;
    if (is_short_circuit_argument)
    {
        /// Arguments of lazy executed function can also be lazy executed.
        for (auto & col : columns)
        {
            if (const ColumnFunction * arg = checkAndGetShortCircuitArgument(col.column))
                col = arg->reduce();
        }
    }

    ColumnWithTypeAndName res{nullptr, function->getResultType(), ""};

    ProfileEvents::increment(ProfileEvents::FunctionExecute);
    if (is_function_compiled)
        ProfileEvents::increment(ProfileEvents::CompiledFunctionExecute);

    res.column = function->execute(columns, res.type, size_);
    return res;
}

const ColumnFunction * checkAndGetShortCircuitArgument(const ColumnPtr & column)
{
    const ColumnFunction * column_function;
    if ((column_function = typeid_cast<const ColumnFunction *>(column.get())) && column_function->isShortCircuitArgument())
        return column_function;
    return nullptr;
}

}
