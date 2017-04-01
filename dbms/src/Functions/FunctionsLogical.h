#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <IO/WriteHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsArithmetic.h>


namespace DB
{

/** Функции - логические связки: and, or, not, xor.
  * Принимают любые числовые типы, возвращают UInt8, содержащий 0 или 1.
  */

template<typename B>
struct AndImpl
{
    static inline bool isSaturable()
    {
        return true;
    }

    static inline bool isSaturatedValue(UInt8 a)
    {
        return !a;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return a && b;
    }
};

template<typename B>
struct OrImpl
{
    static inline bool isSaturable()
    {
        return true;
    }

    static inline bool isSaturatedValue(UInt8 a)
    {
        return a;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return a || b;
    }
};

template<typename B>
struct XorImpl
{
    static inline bool isSaturable()
    {
        return false;
    }

    static inline bool isSaturatedValue(UInt8 a)
    {
        return false;
    }

    static inline UInt8 apply(UInt8 a, B b)
    {
        return (!a) != (!b);
    }
};

template<typename A>
struct NotImpl
{
    using ResultType = UInt8;

    static inline UInt8 apply(A a)
    {
        return !a;
    }
};


using UInt8Container = ColumnUInt8::Container_t;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;


template <typename Op, size_t N>
struct AssociativeOperationImpl
{
    /// Выбрасывает N последних столбцов из in (если их меньше, то все) и кладет в result их комбинацию.
    static void execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        if (N > in.size())
        {
            AssociativeOperationImpl<Op, N - 1>::execute(in, result);
            return;
        }

        AssociativeOperationImpl<Op, N> operation(in);
        in.erase(in.end() - N, in.end());

        size_t n = result.size();
        for (size_t i = 0; i < n; ++i)
        {
            result[i] = operation.apply(i);
        }
    }

    const UInt8Container & vec;
    AssociativeOperationImpl<Op, N - 1> continuation;

    /// Запоминает последние N столбцов из in.
    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), continuation(in) {}

    /// Возвращает комбинацию значений в i-й строке всех столбцов, запомненных в конструкторе.
    inline UInt8 apply(size_t i) const
    {
        if (Op::isSaturable())
        {
            UInt8 a = vec[i];
            return Op::isSaturatedValue(a) ? a : continuation.apply(i);
        }
        else
        {
            return Op::apply(vec[i], continuation.apply(i));
        }
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    const UInt8Container & vec;

    AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - 1]->getData()) {}

    inline UInt8 apply(size_t i) const
    {
        return vec[i];
    }
};


template <template <typename> class Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionAnyArityLogical>(); };

private:
    bool extractConstColumns(ColumnPlainPtrs & in, UInt8 & res)
    {
        bool has_res = false;
        for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i)
        {
            if (in[i]->isConst())
            {
                Field val = (*in[i])[0];
                UInt8 x = !!val.get<UInt64>();
                if (has_res)
                {
                    res = Impl<UInt8>::apply(res, x);
                }
                else
                {
                    res = x;
                    has_res = true;
                }

                in.erase(in.begin() + i);
            }
        }
        return has_res;
    }

    template <typename T>
    bool convertTypeToUInt8(const IColumn * column, UInt8Container & res)
    {
        auto col = typeid_cast<const ColumnVector<T> *>(column);
        if (!col)
            return false;
        const typename ColumnVector<T>::Container_t & vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = !!vec[i];
        }
        return true;
    }

    void convertToUInt8(const IColumn * column, UInt8Container & res)
    {
        if (!convertTypeToUInt8<  Int8 >(column, res) &&
            !convertTypeToUInt8<  Int16>(column, res) &&
            !convertTypeToUInt8<  Int32>(column, res) &&
            !convertTypeToUInt8<  Int64>(column, res) &&
            !convertTypeToUInt8< UInt16>(column, res) &&
            !convertTypeToUInt8< UInt32>(column, res) &&
            !convertTypeToUInt8< UInt64>(column, res) &&
            !convertTypeToUInt8<Float32>(column, res) &&
            !convertTypeToUInt8<Float64>(column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename T>
    bool executeUInt8Type(const UInt8Container & uint8_vec, IColumn * column, UInt8Container & res)
    {
        auto col = typeid_cast<const ColumnVector<T> *>(column);
        if (!col)
            return false;
        const typename ColumnVector<T>::Container_t & other_vec = col->getData();
        size_t n = res.size();
        for (size_t i = 0; i < n; ++i)
        {
            res[i] = Impl<T>::apply(uint8_vec[i], other_vec[i]);
        }
        return true;
    }

    void executeUInt8Other(const UInt8Container & uint8_vec, IColumn * column, UInt8Container & res)
    {
        if (!executeUInt8Type<  Int8 >(uint8_vec, column, res) &&
            !executeUInt8Type<  Int16>(uint8_vec, column, res) &&
            !executeUInt8Type<  Int32>(uint8_vec, column, res) &&
            !executeUInt8Type<  Int64>(uint8_vec, column, res) &&
            !executeUInt8Type< UInt16>(uint8_vec, column, res) &&
            !executeUInt8Type< UInt32>(uint8_vec, column, res) &&
            !executeUInt8Type< UInt64>(uint8_vec, column, res) &&
            !executeUInt8Type<Float32>(uint8_vec, column, res) &&
            !executeUInt8Type<Float64>(uint8_vec, column, res))
            throw Exception("Unexpected type of column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!arguments[i]->isNumeric())
                throw Exception("Illegal type ("
                    + arguments[i]->getName()
                    + ") of " + toString(i + 1) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        ColumnPlainPtrs in(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            in[i] = block.safeGetByPosition(arguments[i]).column.get();
        }
        size_t n = in[0]->size();

        /// Скомбинируем все константные столбцы в одно значение.
        UInt8 const_val = 0;
        bool has_consts = extractConstColumns(in, const_val);

        // Если это значение однозначно определяет результат, вернем его.
        if (has_consts && (in.empty() || Impl<UInt8>::apply(const_val, 0) == Impl<UInt8>::apply(const_val, 1)))
        {
            if (!in.empty())
                const_val = Impl<UInt8>::apply(const_val, 0);
            auto col_res = std::make_shared<ColumnConst<UInt8>>(n, const_val);
            block.safeGetByPosition(result).column = col_res;
            return;
        }

        /// Если это значение - нейтральный элемент, забудем про него.
        if (has_consts && Impl<UInt8>::apply(const_val, 0) == 0 && Impl<UInt8>::apply(const_val, 1) == 1)
            has_consts = false;

        auto col_res = std::make_shared<ColumnUInt8>();
        block.safeGetByPosition(result).column = col_res;
        UInt8Container & vec_res = col_res->getData();

        if (has_consts)
        {
            vec_res.assign(n, const_val);
            in.push_back(col_res.get());
        }
        else
        {
            vec_res.resize(n);
        }

        /// Разделим входные столбцы на UInt8 и остальные. Первые обработаем более эффективно.
        /// col_res в каждый момент будет либо находится в конце uint8_in, либо не содержаться в uint8_in.
        UInt8ColumnPtrs uint8_in;
        ColumnPlainPtrs other_in;
        for (IColumn * column : in)
        {
            if (auto uint8_column = typeid_cast<const ColumnUInt8 *>(column))
                uint8_in.push_back(uint8_column);
            else
                other_in.push_back(column);
        }

        /// Нужен хотя бы один столбец в uint8_in, чтобы было с кем комбинировать столбцы из other_in.
        if (uint8_in.empty())
        {
            if (other_in.empty())
                throw Exception("Hello, I'm a bug", ErrorCodes::LOGICAL_ERROR);

            convertToUInt8(other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in.push_back(col_res.get());
        }

        /// Эффективно скомбинируем все столбцы правильного типа.
        while (uint8_in.size() > 1)
        {
            /// При большом размере блока объединять по 6 толбцов за проход быстрее всего.
            /// При маленьком - чем больше, тем быстрее.
            AssociativeOperationImpl<Impl<UInt8>, 10>::execute(uint8_in, vec_res);
            uint8_in.push_back(col_res.get());
        }

        /// По одному добавим все столбцы неправильного типа.
        while (!other_in.empty())
        {
            executeUInt8Other(uint8_in[0]->getData(), other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in[0] = col_res.get();
        }

        /// Такое возможно, если среди аргументов ровно один неконстантный, и он имеет тип UInt8.
        if (uint8_in[0] != col_res.get())
        {
            vec_res.assign(uint8_in[0]->getData());
        }
    }
};


template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionUnaryLogical>(); };

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (ColumnVector<T> * col = typeid_cast<ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = col_res;

            typename ColumnUInt8::Container_t & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T, Impl<T> >::vector(col->getData(), vec_res);

            return true;
        }
        else if (ColumnConst<T> * col = typeid_cast<ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            UInt8 res = 0;
            UnaryOperationImpl<T, Impl<T> >::constant(col->getData(), res);

            auto col_res = std::make_shared<ColumnConst<UInt8>>(col->size(), res);
            block.safeGetByPosition(result).column = col_res;

            return true;
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isNumeric())
            throw Exception("Illegal type ("
                + arguments[0]->getName()
                + ") of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeType<UInt8>(block, arguments, result)
            ||    executeType<UInt16>(block, arguments, result)
            ||    executeType<UInt32>(block, arguments, result)
            ||    executeType<UInt64>(block, arguments, result)
            ||    executeType<Int8>(block, arguments, result)
            ||    executeType<Int16>(block, arguments, result)
            ||    executeType<Int32>(block, arguments, result)
            ||    executeType<Int64>(block, arguments, result)
            ||    executeType<Float32>(block, arguments, result)
            ||    executeType<Float64>(block, arguments, result)))
           throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


struct NameAnd    { static constexpr auto name = "and"; };
struct NameOr    { static constexpr auto name = "or"; };
struct NameXor    { static constexpr auto name = "xor"; };
struct NameNot    { static constexpr auto name = "not"; };

using FunctionAnd = FunctionAnyArityLogical    <AndImpl,    NameAnd>;
using FunctionOr = FunctionAnyArityLogical    <OrImpl,    NameOr>    ;
using FunctionXor = FunctionAnyArityLogical    <XorImpl,    NameXor>;
using FunctionNot = FunctionUnaryLogical    <NotImpl,    NameNot>;

}
