#include <Functions/array/createArrayLimitGetter.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/AccurateComparison.h>
#include <Functions/castTypeToEither.h>
#include <Common/Exception.h>
#include <base/extended_types.h>
#include <base/wide_integer_to_string.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Reads the limit from a column of a concrete integer type. Rejects negative values (signed types);
/// a value too large for `size_t` becomes `SIZE_MAX`.
template <typename ColumnType>
class TypedArrayLimitGetter final : public ArrayLimitGetter
{
public:
    TypedArrayLimitGetter(const ColumnType & column_, const char * function_name_)
        : column(column_), function_name(function_name_)
    {
    }

    size_t get(size_t row) const override
    {
        using T = typename ColumnType::ValueType;
        const T value = column.getData()[row];

        if constexpr (is_signed_v<T>)
        {
            if (value < 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Argument of function {} must be non-negative, got {}",
                    function_name,
                    value);
        }

        /// A limit too large for `size_t` becomes `SIZE_MAX` (the caller clamps it to the array size).
        if (accurate::greaterOrEqualsOp(value, std::numeric_limits<size_t>::max()))
            return std::numeric_limits<size_t>::max();
        return static_cast<size_t>(value);
    }

private:
    const ColumnType & column;
    const char * function_name;
};

/// Returns the same limit for every row (a constant argument).
class ConstArrayLimitGetter final : public ArrayLimitGetter
{
public:
    explicit ConstArrayLimitGetter(size_t limit_) : limit(limit_) { }

    size_t get(size_t /*row*/) const override { return limit; }

    std::optional<size_t> tryGetConstant() const override { return limit; }

private:
    size_t limit;
};

}

std::unique_ptr<ArrayLimitGetter> createArrayLimitGetter(const IColumn & column, const char * function_name)
{
    /// A constant argument (the common case, e.g. `arrayPartialSort(2, arr)`) is read just once.
    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
    {
        auto data_getter = createArrayLimitGetter(column_const->getDataColumn(), function_name);
        return std::make_unique<ConstArrayLimitGetter>(data_getter->get(0));
    }

    std::unique_ptr<ArrayLimitGetter> getter;
    const bool dispatched = castTypeToEither<
        ColumnUInt8,
        ColumnUInt16,
        ColumnUInt32,
        ColumnUInt64,
        ColumnUInt128,
        ColumnUInt256,
        ColumnInt8,
        ColumnInt16,
        ColumnInt32,
        ColumnInt64,
        ColumnInt128,
        ColumnInt256>(
        &column,
        [&](const auto & typed_column)
        {
            getter = std::make_unique<TypedArrayLimitGetter<std::decay_t<decltype(typed_column)>>>(typed_column, function_name);
            return true;
        });

    if (!dispatched)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Argument of function {} must be an integer column, got {}",
            function_name,
            column.getName());

    return getter;
}

}
