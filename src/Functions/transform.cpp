#include <mutex>
#include <base/bit_cast.h>

#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/typeid_cast.h>
#include <base/StringRef.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

template <typename T>
struct ColumnNumericImpl
{
    using type = ColumnVector<T>;
};

template <class T>
concept decimals = std::is_same_v<T, Decimal32> || std::is_same_v<T, Decimal64> || std::is_same_v<T, DateTime64>;

template <decimals T>
struct ColumnNumericImpl<T>
{
    using type = ColumnDecimal<T>;
};

template <typename T>
using ColumnNumeric = typename ColumnNumericImpl<T>::type;

enum class DestSource
{
    SOURCE,
    DEFAULT_ELEMENT,
    DEFAULT_COLUMN,
};

template <typename Func>
bool dispatchNumericColumns(Func && func)
{
#define APPLY_FOR_NUMERIC_COLUMNS(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64) \
    M(Decimal32) \
    M(Decimal64) \
    M(DateTime64)

#define M(NAME) else if (func(std::type_identity_t<NAME>{})) return true;
    if (false) {} // NOLINT
    APPLY_FOR_NUMERIC_COLUMNS(M)
#undef M
#undef APPLY_FOR_NUMERIC_COLUMNS

    return false;
}

template <typename Func>
void dispatchNullableCases(bool src_has_null, bool dst_has_null, Func && func)
{
    if (src_has_null && dst_has_null)
        func(std::true_type(), std::true_type());
    else if (src_has_null)
        func(std::true_type(), std::false_type());
    else if (dst_has_null)
        func(std::false_type(), std::true_type());
    else
        func(std::false_type(), std::false_type());
}

namespace
{

/** transform(x, from_array, to_array[, default]) - convert x according to an explicitly passed match.
  */

/** transform(x, [from...], [to...], default)
  * - converts the values according to the explicitly specified mapping.
  *
  * x - what to transform.
  * from - a constant array of values for the transformation.
  * to - a constant array of values into which values from `from` must be transformed.
  * default - what value to use if x is not equal to any of the values in `from`.
  * `from` and `to` - arrays of the same size.
  *
  * Types:
  * transform(T, Array(T), Array(U), U) -> U
  *
  * transform(x, [from...], [to...])
  * - if `default` is not specified, then for values of `x` for which there is no corresponding element in `from`, the unchanged value of `x` is returned.
  *
  * Types:
  * transform(T, Array(T), Array(T)) -> T
  *
  * Note: the implementation is rather cumbersome.
  */
class FunctionTransform : public IFunction
{
public:
    static constexpr auto name = "transform";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTransform>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto args_size = arguments.size();
        if (args_size != 3 && args_size != 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
                getName(),
                args_size);

        const auto & type_x_maybe_null = arguments[0];
        auto type_x = type_x_maybe_null;
        if (type_x->isNullable())
            type_x = static_cast<const DataTypeNullable &>(*type_x).getNestedType();

        if (!type_x_maybe_null->onlyNull() && !type_x->isValueRepresentedByNumber() && !isString(type_x))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Unsupported type {} of first argument of function {}, must be numeric type or Date/DateTime or String",
                type_x->getName(),
                getName());

        const DataTypeArray * type_arr_from = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!type_arr_from)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be array of source values to transform from",
                getName());

        /// NULL in src value array is useless. (NULL != NULL)
        const auto & type_arr_from_nested_maybe_null = type_arr_from->getNestedType();
        auto type_arr_from_nested = type_arr_from_nested_maybe_null;
        if (type_arr_from_nested->isNullable())
            type_arr_from_nested = static_cast<const DataTypeNullable &>(*type_arr_from_nested).getNestedType();

        if (!type_x_maybe_null->onlyNull() && !type_arr_from_nested_maybe_null->onlyNull())
        {
            if ((type_x->isValueRepresentedByNumber() != type_arr_from_nested->isValueRepresentedByNumber())
                || (isString(type_x) != isString(type_arr_from_nested)))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument and elements of array of second argument of function {} must have compatible types: both numeric or "
                    "both strings",
                    getName());
        }

        const auto * type_arr_to = checkAndGetDataType<DataTypeArray>(arguments[2].get());

        if (!type_arr_to)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument of function {} must be array of destination values to transform to",
                getName());

        const auto & type_arr_to_nested_maybe_null = type_arr_to->getNestedType();
        auto type_arr_to_nested = type_arr_to_nested_maybe_null;
        if (type_arr_to_nested->isNullable())
            type_arr_to_nested = static_cast<const DataTypeNullable &>(*type_arr_to_nested).getNestedType();

        if (args_size == 3)
        {
            if (!type_x_maybe_null->onlyNull() && !type_arr_to_nested_maybe_null->onlyNull())
            {
                if ((type_x->isValueRepresentedByNumber() != type_arr_to_nested->isValueRepresentedByNumber())
                    || (isString(type_x) != isString(type_arr_to_nested)))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} has signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; "
                        "where T and U are types",
                        getName());
            }

            return getLeastSupertype(DataTypes{type_x_maybe_null, type_arr_to_nested_maybe_null});
        }
        else
        {
            const DataTypePtr & type_default_maybe_null = arguments[3];
            if (type_arr_to_nested_maybe_null->onlyNull() || type_default_maybe_null->onlyNull())
                return getLeastSupertype(DataTypes{type_arr_to_nested_maybe_null, type_default_maybe_null});

            DataTypePtr type_default = type_default_maybe_null;
            if (type_default->isNullable())
                type_default = static_cast<const DataTypeNullable &>(*type_default).getNestedType();

            if (!type_default->isValueRepresentedByNumber() && !isString(type_default))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Unsupported type {} of fourth argument (default value) of function {}, must be numeric type or Date/DateTime or "
                    "String",
                    type_default->getName(),
                    getName());

            bool default_is_string = WhichDataType(type_default).isString();
            bool nested_is_string = WhichDataType(type_arr_to_nested).isString();

            if ((type_default->isValueRepresentedByNumber() != type_arr_to_nested->isValueRepresentedByNumber())
                || (default_is_string != nested_is_string))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} has signature: transform(T, Array(T), Array(U), U) -> U; or transform(T, Array(T), Array(T)) -> T; where "
                    "T and U are types",
                    getName());

            return getLeastSupertype(DataTypes{type_arr_to_nested_maybe_null, type_default_maybe_null});
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnConst * array_from = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        const ColumnConst * array_to = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());

        if (!array_from || !array_to)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second and third arguments of function {} must be constant arrays", getName());

        initialize(array_from->getValue<Array>(), array_to->getValue<Array>(), arguments);

        const auto * in = arguments.front().column.get();

        if (isColumnConst(*in))
            return executeConst(arguments, result_type, input_rows_count);

        const IColumn * default_column = nullptr;
        if (arguments.size() == 4)
            default_column = arguments[3].column.get();

        if (in->onlyNull() || cache.src_all_null)
        {
            if (!default_column)
                return castColumn(arguments[0], result_type);
            else
                return castColumn(arguments[3], result_type);
        }

        auto column_result = result_type->createColumn();
        auto * out = column_result.get();

        ConstNullMapPtr in_null_map = nullptr;
        NullMap * out_null_map = nullptr;
        if (in->isNullable())
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*in);
            in = &nullable_col.getNestedColumn();
            in_null_map = &nullable_col.getNullMapData();
        }

        if (out->isNullable())
        {
            auto & nullable_col = static_cast<ColumnNullable &>(*out);
            out = &nullable_col.getNestedColumn();
            out_null_map = &nullable_col.getNullMapData();
        }

        const bool execute_numeric_succeeded = dispatchNumericColumns(
            [&](auto type) { return executeNum<decltype(type)>(in, in_null_map, out, out_null_map, default_column); });

        if (!execute_numeric_succeeded && !executeString(in, in_null_map, out, out_null_map, default_column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", in->getName(), getName());

        return column_result;
    }

private:
    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
    {
        /// Materialize the input column and compute the function as usual.

        ColumnsWithTypeAndName args = arguments;
        args[0].column = args[0].column->cloneResized(input_rows_count)->convertToFullColumnIfConst();

        auto impl = FunctionToOverloadResolverAdaptor(std::make_shared<FunctionTransform>()).build(args);

        return impl->execute(args, result_type, input_rows_count);
    }

    template <typename T>
    bool executeNum(
        const IColumn * in_untyped,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        if (const auto in = checkAndGetColumn<ColumnNumeric<T>>(in_untyped))
        {
            if (!default_untyped)
            {
                auto out = typeid_cast<ColumnNumeric<T> *>(out_untyped);
                if (!out)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of elements of array of third argument of function {}, must be {}",
                        out_untyped->getName(),
                        getName(),
                        in->getName());

                executeImplNumToNumWithoutDefault(in->getData(), in_null_map, out->getData(), out_null_map);
            }
            else
            {
                const bool execute_numeric_succeeded = dispatchNumericColumns(
                    [&](auto type)
                    { return executeNumToNum<T, decltype(type)>(in, in_null_map, out_untyped, out_null_map, default_untyped); });

                if (!execute_numeric_succeeded && !executeNumToString<T>(in, in_null_map, out_untyped, out_null_map, default_untyped))
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of elements of array of second argument of function {}",
                        in->getName(),
                        getName());
            }

            return true;
        }

        return false;
    }

    bool executeString(
        const IColumn * in_untyped,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        if (const auto * in = checkAndGetColumn<ColumnString>(in_untyped))
        {
            if (!executeStringToString(in, in_null_map, out_untyped, out_null_map, default_untyped))
            {
                const bool execute_numeric_succeeded = dispatchNumericColumns(
                    [&](auto type)
                    { return executeStringToNum<decltype(type)>(in, in_null_map, out_untyped, out_null_map, default_untyped); });

                if (!execute_numeric_succeeded)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of elements of array of second argument of function {}",
                        in->getName(),
                        getName());
            }

            return true;
        }

        return false;
    }

    template <typename T, typename U>
    bool executeNumToNum(
        const ColumnNumeric<T> * in,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        auto out = typeid_cast<ColumnNumeric<U> *>(out_untyped);
        if (!out)
            return false;

        if (isColumnConst(*default_untyped))
        {
            executeImplNumToNumWithDefaultValue(
                in->getData(), in_null_map, out->getData(), out_null_map, cache.const_default_value);
        }
        else
        {
            ConstNullMapPtr default_null_map = nullptr;
            if (default_untyped->isNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*default_untyped);
                default_untyped = &nullable_col.getNestedColumn();
                default_null_map = &nullable_col.getNullMapData();
            }

            const bool execute_numeric_succeeded = dispatchNumericColumns(
                [&](auto type)
                {
                    return executeNumToNumWithNonConstDefault<T, U, decltype(type)>(
                        in, in_null_map, out, out_null_map, default_untyped, default_null_map);
                });

            if (!execute_numeric_succeeded)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of fourth argument of function {}",
                    default_untyped->getName(),
                    getName());
        }

        return true;
    }

    template <typename T, typename U, typename V>
    bool executeNumToNumWithNonConstDefault(
        const ColumnNumeric<T> * in,
        ConstNullMapPtr in_null_map,
        ColumnNumeric<U> * out,
        NullMapPtr out_null_map,
        const IColumn * default_untyped,
        ConstNullMapPtr default_null_map) const
    {
        auto col_default = checkAndGetColumn<ColumnNumeric<V>>(default_untyped);
        if (!col_default)
            return false;

        executeImplNumToNumWithDefaultColumn(
            in->getData(), in_null_map, out->getData(), out_null_map, col_default->getData(), default_null_map);
        return true;
    }

    template <typename T>
    bool executeNumToString(
        const ColumnNumeric<T> * in,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        auto * out = typeid_cast<ColumnString *>(out_untyped);
        if (!out)
            return false;

        if (isColumnConst(*default_untyped))
        {
            executeImplNumToStringWithDefaultValue(
                in->getData(), in_null_map, out->getChars(), out->getOffsets(), out_null_map, cache.const_default_value);
        }
        else
        {
            ConstNullMapPtr default_null_map = nullptr;
            if (default_untyped->isNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*default_untyped);
                default_untyped = &nullable_col.getNestedColumn();
                default_null_map = &nullable_col.getNullMapData();
            }

            const auto * default_col = checkAndGetColumn<ColumnString>(default_untyped);
            if (!default_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of fourth argument of function {}",
                    default_untyped->getName(),
                    getName());

            executeImplNumToStringWithDefaultColumn(
                in->getData(),
                in_null_map,
                out->getChars(),
                out->getOffsets(),
                out_null_map,
                default_col->getChars(),
                default_col->getOffsets(),
                default_null_map);
        }

        return true;
    }

    template <typename U>
    bool executeStringToNum(
        const ColumnString * in,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        auto out = typeid_cast<ColumnNumeric<U> *>(out_untyped);
        if (!out)
            return false;

        if (isColumnConst(*default_untyped))
        {
            executeImplStringToNumWithDefaultValue(
                in->getChars(), in->getOffsets(), in_null_map, out->getData(), out_null_map, cache.const_default_value);
        }
        else
        {
            ConstNullMapPtr default_null_map = nullptr;
            if (default_untyped->isNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*default_untyped);
                default_untyped = &nullable_col.getNestedColumn();
                default_null_map = &nullable_col.getNullMapData();
            }

            const bool execute_numeric_succeeded = dispatchNumericColumns(
                [&](auto type)
                {
                    return executeStringToNumWithNonConstDefault<U, decltype(type)>(
                        in, in_null_map, out, out_null_map, default_untyped, default_null_map);
                });

            if (!execute_numeric_succeeded)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of fourth argument of function {}",
                    default_untyped->getName(),
                    getName());
        }

        return true;
    }

    template <typename U, typename V>
    bool executeStringToNumWithNonConstDefault(
        const ColumnString * in,
        ConstNullMapPtr in_null_map,
        ColumnNumeric<U> * out,
        NullMapPtr out_null_map,
        const IColumn * default_untyped,
        ConstNullMapPtr default_null_map) const
    {
        auto col_default = checkAndGetColumn<ColumnNumeric<V>>(default_untyped);
        if (!col_default)
            return false;

        executeImplStringToNumWithDefaultColumn(
            in->getChars(), in->getOffsets(), in_null_map, out->getData(), out_null_map, col_default->getData(), default_null_map);

        return true;
    }

    bool executeStringToString(
        const ColumnString * in,
        ConstNullMapPtr in_null_map,
        IColumn * out_untyped,
        NullMapPtr out_null_map,
        const IColumn * default_untyped) const
    {
        auto * out = typeid_cast<ColumnString *>(out_untyped);
        if (!out)
            return false;

        if (!default_untyped)
        {
            executeImplStringToStringWithoutDefault(
                in->getChars(), in->getOffsets(), in_null_map, out->getChars(), out->getOffsets(), out_null_map);
        }
        else if (isColumnConst(*default_untyped))
        {
            executeImplStringToStringWithDefaultValue(
                in->getChars(),
                in->getOffsets(),
                in_null_map,
                out->getChars(),
                out->getOffsets(),
                out_null_map,
                cache.const_default_value);
        }
        else
        {
            ConstNullMapPtr default_null_map = nullptr;
            if (default_untyped->isNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*default_untyped);
                default_untyped = &nullable_col.getNestedColumn();
                default_null_map = &nullable_col.getNullMapData();
            }

            const auto * default_col = checkAndGetColumn<ColumnString>(default_untyped);
            if (!default_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of fourth argument of function {}",
                    default_untyped->getName(),
                    getName());

            executeImplStringToStringWithDefaultColumn(
                in->getChars(),
                in->getOffsets(),
                in_null_map,
                out->getChars(),
                out->getOffsets(),
                out_null_map,
                default_col->getChars(),
                default_col->getOffsets(),
                default_null_map);
        }

        return true;
    }

    template <typename T, typename U, typename V>
    void executeImplNumToNumWithDefaultColumn(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map,
        const PaddedPODArray<V> & dst_defaults,
        ConstNullMapPtr dst_defaults_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_defaults_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplNumToNum<DestSource::DEFAULT_COLUMN, k_src_has_null, k_dst_has_null>(
                    src, src_null_map, dst, dst_null_map, {}, dst_defaults, dst_defaults_null_map);
            });
    }

    template <typename T, typename U>
    void executeImplNumToNumWithDefaultValue(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map,
        Field dst_default) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default.isNull();

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplNumToNum<DestSource::DEFAULT_ELEMENT, k_src_has_null, k_dst_has_null>(
                    src, src_null_map, dst, dst_null_map, dst_default);
            });
    }

    template <typename T, typename U>
    void executeImplNumToNumWithoutDefault(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            { executeImplNumToNum<DestSource::SOURCE, k_src_has_null, k_dst_has_null>(src, src_null_map, dst, dst_null_map); });
    }

    template <DestSource source, bool src_has_null, bool dst_has_null, typename T, typename U, typename V = U>
    void executeImplNumToNum(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map [[maybe_unused]],
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map [[maybe_unused]],
        Field dst_default [[maybe_unused]] = {},
        const PaddedPODArray<V> & dst_defaults [[maybe_unused]] = {},
        ConstNullMapPtr dst_defaults_null_map [[maybe_unused]] = {}) const
    {
        const auto & table = *cache.table_num_to_num;
        size_t size = src.size();
        dst.resize(size);
        if constexpr (dst_has_null)
        {
            if (dst_null_map)
                dst_null_map->resize_fill(size);
        }

        U dst_default_value{};
        if (!dst_default.isNull())
            dst_default_value = static_cast<U>(dst_default.get<U>());

        for (size_t i = 0; i < size; ++i)
        {
            auto fill_default = [&]()
            {
                if constexpr (source == DestSource::SOURCE)
                {
                    if constexpr (src_has_null)
                    {
                        if ((*src_null_map)[i]) // NOLINT
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            dst[i] = 0;
                            return;
                        }
                    }

                    dst[i] = src[i];
                }
                else if constexpr (source == DestSource::DEFAULT_COLUMN)
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_defaults_null_map && (*dst_defaults_null_map)[i])
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            dst[i] = 0;
                            return;
                        }
                    }

                    if constexpr (is_decimal<U>)
                        dst[i] = static_cast<typename U::NativeType>(dst_defaults[i]);
                    else
                        dst[i] = static_cast<U>(dst_defaults[i]); // NOLINT(bugprone-signed-char-misuse,cert-str34-c)
                }
                else
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default.isNull())
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            dst[i] = 0;
                            return;
                        }
                    }

                    dst[i] = dst_default_value;
                }
            };

            if constexpr (src_has_null)
            {
                if ((*src_null_map)[i]) // NOLINT
                {
                    fill_default();
                    continue;
                }
            }

            auto key = bit_cast<UInt64>(src[i]);

            if constexpr (dst_has_null)
            {
                if (dst_null_map && cache.null_value_num_set && cache.null_value_num_set->has(key))
                {
                    (*dst_null_map)[i] = 1;
                    dst[i] = 0;
                    continue;
                }
            }

            const auto * it = table.find(key);
            if (it)
                memcpy(&dst[i], &it->getMapped(), sizeof(dst[i])); /// little endian.
            else
                fill_default();
        }
    }

    template <typename T>
    void executeImplNumToStringWithDefaultColumn(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map,
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map,
        const ColumnString::Chars & dst_default_data,
        const ColumnString::Offsets & dst_default_offsets,
        ConstNullMapPtr dst_default_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplNumToString<false, k_src_has_null, k_dst_has_null, T>(
                    src,
                    src_null_map,
                    dst_data,
                    dst_offsets,
                    dst_null_map,
                    {},
                    dst_default_data,
                    dst_default_offsets,
                    dst_default_null_map);
            });
    }

    template <typename T>
    void executeImplNumToStringWithDefaultValue(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map,
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map,
        Field dst_default) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default.isNull();

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplNumToString<true, k_src_has_null, k_dst_has_null, T>(
                    src, src_null_map, dst_data, dst_offsets, dst_null_map, dst_default);
            });
    }

    template <bool with_default_value, bool src_has_null, bool dst_has_null, typename T>
    void executeImplNumToString(
        const PaddedPODArray<T> & src,
        ConstNullMapPtr src_null_map [[maybe_unused]],
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map [[maybe_unused]],
        Field dst_default [[maybe_unused]],
        const ColumnString::Chars & dst_default_data [[maybe_unused]] = {},
        const ColumnString::Offsets & dst_default_offsets [[maybe_unused]] = {},
        ConstNullMapPtr dst_default_null_map [[maybe_unused]] = {}) const
    {
        const auto & table = *cache.table_num_to_string;
        size_t size = src.size();
        dst_offsets.resize(size);

        if constexpr (dst_has_null)
        {
            if (dst_null_map)
                dst_null_map->resize_fill(size);
        }

        StringRef dst_default_value{};
        if (!dst_default.isNull())
        {
            const String & default_str = dst_default.get<const String &>();
            dst_default_value = {default_str.data(), default_str.size() + 1};
        }

        ColumnString::Offset current_dst_offset = 0;
        ColumnString::Offset current_dst_default_offset [[maybe_unused]] = 0;
        for (size_t i = 0; i < size; ++i)
        {
            StringRef ref;

            auto fill_default = [&]()
            {
                if constexpr (with_default_value)
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default.isNull())
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            return;
                        }
                    }

                    ref = dst_default_value;
                }
                else
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default_null_map && (*dst_default_null_map)[i])
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            return;
                        }
                    }

                    ref.data = reinterpret_cast<const char *>(&dst_default_data[current_dst_default_offset]);
                    ref.size = dst_default_offsets[i] - current_dst_default_offset;
                }
            };

            auto fill_dst = [&]()
            {
                if constexpr (src_has_null)
                {
                    if ((*src_null_map)[i]) // NOLINT
                    {
                        fill_default();
                        return;
                    }
                }

                auto key = bit_cast<UInt64>(src[i]);

                if constexpr (dst_has_null)
                {
                    if (dst_null_map && cache.null_value_num_set && cache.null_value_num_set->has(key))
                    {
                        (*dst_null_map)[i] = 1;
                        return;
                    }
                }

                if (const auto * it = table.find(key))
                    ref = it->getMapped();
                else
                    fill_default();
            };

            fill_dst();

            if (ref.size > 0)
            {
                dst_data.resize(current_dst_offset + ref.size);
                memcpy(&dst_data[current_dst_offset], ref.data, ref.size);
                current_dst_offset += ref.size;
            }
            else
            {
                dst_data.push_back('\0');
                ++current_dst_offset;
            }

            if constexpr (!with_default_value)
                current_dst_default_offset = dst_default_offsets[i];
            dst_offsets[i] = current_dst_offset;
        }
    }

    template <typename U, typename V>
    void executeImplStringToNumWithDefaultColumn(
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map,
        const PaddedPODArray<V> & dst_defaults,
        ConstNullMapPtr dst_defaults_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_defaults_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplStringToNum<false, k_src_has_null, k_dst_has_null>(
                    src_data, src_offsets, src_null_map, dst, dst_null_map, {}, dst_defaults, dst_defaults_null_map);
            });
    }

    template <typename U>
    void executeImplStringToNumWithDefaultValue(
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map,
        Field dst_default) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default.isNull();

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplStringToNum<true, k_src_has_null, k_dst_has_null>(
                    src_data, src_offsets, src_null_map, dst, dst_null_map, dst_default);
            });
    }

    template <bool with_default, bool src_has_null, bool dst_has_null, typename U, typename V = U>
    void executeImplStringToNum(
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        PaddedPODArray<U> & dst,
        NullMapPtr dst_null_map,
        Field dst_default [[maybe_unused]],
        const PaddedPODArray<V> & dst_defaults [[maybe_unused]] = {},
        ConstNullMapPtr dst_defaults_null_map [[maybe_unused]] = {}) const
    {
        const auto & table = *cache.table_string_to_num;
        size_t size = src_offsets.size();
        dst.resize(size);

        if constexpr (dst_has_null)
        {
            if (dst_null_map)
                dst_null_map->resize_fill(size);
        }

        U dst_default_value{};
        if (!dst_default.isNull())
            dst_default_value = static_cast<U>(dst_default.get<U>());

        ColumnString::Offset current_src_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            StringRef key{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
            current_src_offset = src_offsets[i];

            auto fill_default = [&]()
            {
                if constexpr (with_default)
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default.isNull())
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            dst[i] = 0;
                            return;
                        }
                    }

                    dst[i] = dst_default_value;
                }
                else
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_defaults_null_map && (*dst_defaults_null_map)[i])
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            dst[i] = 0;
                            return;
                        }
                    }

                    if constexpr (is_decimal<U>)
                        dst[i] = static_cast<typename U::NativeType>(dst_defaults[i]);
                    else
                        dst[i] = static_cast<U>(dst_defaults[i]); // NOLINT(bugprone-signed-char-misuse,cert-str34-c)
                }
            };

            if constexpr (src_has_null)
            {
                if ((*src_null_map)[i])
                {
                    fill_default();
                    continue;
                }
            }

            if constexpr (dst_has_null)
            {
                if (dst_null_map && cache.null_value_string_set && cache.null_value_string_set->has(key))
                {
                    (*dst_null_map)[i] = 1;
                    dst[i] = 0;
                    continue;
                }
            }

            const auto * it = table.find(key);
            if (it)
                memcpy(&dst[i], &it->getMapped(), sizeof(dst[i])); /// little endian.
            else
                fill_default();
        }
    }

    void executeImplStringToStringWithDefaultColumn( /// NOLINT
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map,
        const ColumnString::Chars & dst_default_data,
        const ColumnString::Offsets & dst_default_offsets,
        ConstNullMapPtr dst_default_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplStringToString<DestSource::DEFAULT_COLUMN, k_src_has_null, k_dst_has_null>(
                    src_data,
                    src_offsets,
                    src_null_map,
                    dst_data,
                    dst_offsets,
                    dst_null_map,
                    {},
                    dst_default_data,
                    dst_default_offsets,
                    dst_default_null_map);
            });
    }

    void executeImplStringToStringWithDefaultValue( /// NOLINT
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map,
        Field dst_default) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr || dst_default.isNull();

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplStringToString<DestSource::DEFAULT_ELEMENT, k_src_has_null, k_dst_has_null>(
                    src_data, src_offsets, src_null_map, dst_data, dst_offsets, dst_null_map, dst_default);
            });
    }

    void executeImplStringToStringWithoutDefault( /// NOLINT
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map,
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map) const
    {
        bool src_has_null = src_null_map != nullptr;
        bool dst_has_null = dst_null_map != nullptr;

        dispatchNullableCases(
            src_has_null,
            dst_has_null,
            [&](auto k_src_has_null, auto k_dst_has_null)
            {
                executeImplStringToString<DestSource::SOURCE, k_src_has_null, k_dst_has_null>(
                    src_data, src_offsets, src_null_map, dst_data, dst_offsets, dst_null_map);
            });
    }

    template <DestSource source, bool src_has_null, bool dst_has_null>
    void executeImplStringToString(
        const ColumnString::Chars & src_data,
        const ColumnString::Offsets & src_offsets,
        ConstNullMapPtr src_null_map [[maybe_unused]],
        ColumnString::Chars & dst_data,
        ColumnString::Offsets & dst_offsets,
        NullMapPtr dst_null_map [[maybe_unused]],
        Field dst_default [[maybe_unused]] = {},
        const ColumnString::Chars & dst_default_data [[maybe_unused]] = {},
        const ColumnString::Offsets & dst_default_offsets [[maybe_unused]] = {},
        ConstNullMapPtr dst_default_null_map [[maybe_unused]] = {}) const
    {
        const auto & table = *cache.table_string_to_string;
        size_t size = src_offsets.size();
        dst_offsets.resize(size);

        if constexpr (dst_has_null)
        {
            if (dst_null_map)
                dst_null_map->resize_fill(size);
        }

        StringRef dst_default_value{};
        if (!dst_default.isNull())
        {
            const String & default_str = dst_default.get<const String &>();
            dst_default_value = {default_str.data(), default_str.size() + 1};
        }

        ColumnString::Offset current_src_offset = 0;
        ColumnString::Offset current_dst_offset = 0;
        ColumnString::Offset current_dst_default_offset [[maybe_unused]] = 0;
        for (size_t i = 0; i < size; ++i)
        {
            StringRef src_ref{&src_data[current_src_offset], src_offsets[i] - current_src_offset};
            current_src_offset = src_offsets[i];
            StringRef dst_ref;

            auto fill_default = [&]()
            {
                if constexpr (source == DestSource::SOURCE)
                {
                    if constexpr (src_has_null)
                    {
                        if ((*src_null_map)[i])
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            return;
                        }
                    }

                    dst_ref = src_ref;
                }
                else if constexpr (source == DestSource::DEFAULT_ELEMENT)
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default.isNull())
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            return;
                        }
                    }

                    dst_ref = dst_default_value;
                }
                else
                {
                    if constexpr (dst_has_null)
                    {
                        if (dst_default_null_map && (*dst_default_null_map)[i])
                        {
                            (*dst_null_map)[i] = 1; // NOLINT
                            return;
                        }
                    }

                    dst_ref.data = reinterpret_cast<const char *>(&dst_default_data[current_dst_default_offset]);
                    dst_ref.size = dst_default_offsets[i] - current_dst_default_offset;
                }
            };

            auto fill_dst = [&]()
            {
                if constexpr (src_has_null)
                {
                    if ((*src_null_map)[i]) // NOLINT
                    {
                        fill_default();
                        return;
                    }
                }

                if constexpr (dst_has_null)
                {
                    if (dst_null_map && cache.null_value_string_set && cache.null_value_string_set->has(src_ref))
                    {
                        (*dst_null_map)[i] = 1;
                        return;
                    }
                }

                const auto * it = table.find(src_ref);
                if (it)
                    dst_ref = it->getMapped();
                else
                    fill_default();
            };

            fill_dst();

            if (dst_ref.size > 0)
            {
                dst_data.resize(current_dst_offset + dst_ref.size);
                memcpy(&dst_data[current_dst_offset], dst_ref.data, dst_ref.size);
                current_dst_offset += dst_ref.size;
            }
            else
            {
                dst_data.push_back('\0');
                ++current_dst_offset;
            }

            if constexpr (source == DestSource::DEFAULT_COLUMN)
                current_dst_default_offset = dst_default_offsets[i];
            dst_offsets[i] = current_dst_offset;
        }
    }


    /// Different versions of the hash tables to implement the mapping.

    struct Cache
    {
        using NumToNum = HashMap<UInt64, UInt64, HashCRC32<UInt64>>;
        using NumToString = HashMap<UInt64, StringRef, HashCRC32<UInt64>>;     /// Everywhere StringRef's with trailing zero.
        using StringToNum = HashMap<StringRef, UInt64, StringRefHash>;
        using StringToString = HashMap<StringRef, StringRef, StringRefHash>;

        using NullSetNum = HashSet<UInt64, HashCRC32<UInt64>>;
        using NullSetString = HashSet<StringRef, StringRefHash>;

        std::unique_ptr<NumToNum> table_num_to_num;
        std::unique_ptr<NumToString> table_num_to_string;
        std::unique_ptr<StringToNum> table_string_to_num;
        std::unique_ptr<StringToString> table_string_to_string;
        bool src_all_null{false};

        std::unique_ptr<NullSetNum> null_value_num_set;
        std::unique_ptr<NullSetString> null_value_string_set;

        Arena string_pool;

        Field const_default_value;    /// Null, if not specified.

        std::atomic<bool> initialized{false};
        std::mutex mutex;
    };

    mutable Cache cache;


    static UInt64 bitCastToUInt64(const Field & x)
    {
        switch (x.getType())
        {
            case Field::Types::UInt64:      return x.get<UInt64>();
            case Field::Types::Int64:       return x.get<Int64>();
            case Field::Types::Float64:     return std::bit_cast<UInt64>(x.get<Float64>());
            case Field::Types::Bool:        return x.get<bool>();
            case Field::Types::Decimal32:   return x.get<DecimalField<Decimal32>>().getValue();
            case Field::Types::Decimal64:   return x.get<DecimalField<Decimal64>>().getValue();
            default:
                throw Exception("Unexpected type in function 'transform'", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    /// Can be called from different threads. It works only on the first call.
    void initialize(const Array & from, const Array & to, const ColumnsWithTypeAndName & arguments) const
    {
        if (cache.initialized)
            return;

        const size_t size = from.size();
        if (0 == size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty arrays are illegal in function {}", getName());

        std::lock_guard lock(cache.mutex);

        if (cache.initialized)
            return;

        if (size != to.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second and third arguments of function {} must be arrays of same size", getName());

        Array converted_to;
        const Array * used_to = &to;

        /// Whether the default value is set.

        if (arguments.size() == 4)
        {
            const IColumn * default_col_maybe_null = arguments[3].column.get();
            const ColumnConst * const_default_col = typeid_cast<const ColumnConst *>(default_col_maybe_null);

            const IColumn * default_col = default_col_maybe_null;
            if (const_default_col)
            {
                cache.const_default_value = (*const_default_col)[0];
                default_col = &const_default_col->getDataColumn();
            }

            if (default_col->isNullable())
                default_col = &static_cast<const ColumnNullable &>(*default_col).getNestedColumn();

            /// Do we need to convert the elements `to` and `default_value` to the smallest common type that is Float64?
            bool default_col_is_float = checkColumn<ColumnFloat32>(default_col) || checkColumn<ColumnFloat64>(default_col);
            bool to_is_float = to[0].getType() == Field::Types::Float64;

            if (default_col_is_float && !to_is_float)
            {
                converted_to.resize(size);
                for (size_t i = 0; i < size; ++i)
                {
                    if (!to[i].isNull())
                        converted_to[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), to[i]);
                }
                used_to = &converted_to;
            }
            else if (!default_col_is_float && to_is_float)
            {
                if (const_default_col && !cache.const_default_value.isNull())
                    cache.const_default_value = applyVisitor(FieldVisitorConvertToNumber<Float64>(), cache.const_default_value);
            }
        }

        /// Note: Doesn't check the duplicates in the `from` array.

        const IDataType & from_type = *arguments[0].type;

        bool has_null = false;
        auto from_arr_element_type = Field::Types::Null;
        auto to_arr_element_type = Field::Types::Null;
        for (size_t i = 0; i < size; ++i)
        {
            if (!from[i].isNull() && from_arr_element_type == Field::Types::Null)
                from_arr_element_type = from[i].getType();

            if ((*used_to)[i].isNull())
                has_null = true;
            else if (to_arr_element_type == Field::Types::Null)
                to_arr_element_type = to[i].getType();
        }

        if (from_arr_element_type == Field::Types::Null)
        {
            /// src all NULL, always use default
            cache.src_all_null = true;
        }
        else if (from_arr_element_type != Field::Types::String)
        {
            if (has_null)
                cache.null_value_num_set = std::make_unique<Cache::NullSetNum>();

            cache.table_num_to_num = std::make_unique<Cache::NumToNum>();
            cache.table_num_to_string = std::make_unique<Cache::NumToString>();

            if (to_arr_element_type != Field::Types::String)
            {
                auto & table = *cache.table_num_to_num;
                for (size_t i = 0; i < size; ++i)
                {
                    Field key = convertFieldToType(from[i], from_type);
                    if (key.isNull())
                        continue;

                    if ((*used_to)[i].isNull())
                        cache.null_value_num_set->insert(bitCastToUInt64(key));
                    else
                        table[bitCastToUInt64(key)] = bitCastToUInt64((*used_to)[i]);
                }
            }
            else
            {
                auto & table = *cache.table_num_to_string;
                for (size_t i = 0; i < size; ++i)
                {
                    Field key = convertFieldToType(from[i], from_type);
                    if (key.isNull())
                        continue;

                    if ((*used_to)[i].isNull())
                        cache.null_value_num_set->insert(bitCastToUInt64(key));
                    else
                    {
                        const String & str_to = to[i].get<const String &>();
                        StringRef ref{cache.string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
                        table[bitCastToUInt64(key)] = ref;
                    }
                }
            }
        }
        else
        {
            if (has_null)
                cache.null_value_string_set = std::make_unique<Cache::NullSetString>();

            cache.table_string_to_num = std::make_unique<Cache::StringToNum>();
            cache.table_string_to_string = std::make_unique<Cache::StringToString>();

            if (to_arr_element_type != Field::Types::String)
            {
                auto & table = *cache.table_string_to_num;
                for (size_t i = 0; i < size; ++i)
                {
                    if (from[i].isNull())
                        continue;

                    const String & str_from = from[i].get<const String &>();
                    StringRef ref{cache.string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};

                    if ((*used_to)[i].isNull())
                        cache.null_value_string_set->insert(ref);
                    else
                        table[ref] = bitCastToUInt64((*used_to)[i]);
                }
            }
            else
            {
                auto & table = *cache.table_string_to_string;
                for (size_t i = 0; i < size; ++i)
                {
                    if (from[i].isNull())
                        continue;

                    const String & str_from = from[i].get<const String &>();
                    StringRef ref_from{cache.string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};

                    if ((*used_to)[i].isNull())
                        cache.null_value_string_set->insert(ref_from);
                    else
                    {
                        const String & str_to = to[i].get<const String &>();
                        StringRef ref_to{cache.string_pool.insert(str_to.data(), str_to.size() + 1), str_to.size() + 1};
                        table[ref_from] = ref_to;
                    }
                }
            }
        }

        cache.initialized = true;
    }
};

}

REGISTER_FUNCTION(Transform)
{
    factory.registerFunction<FunctionTransform>();
}

}
