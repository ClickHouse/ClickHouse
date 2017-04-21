#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsArithmetic.h>
#include <Functions/IFunction.h>


namespace DB
{

/** Функции поиска и замены в строках:
  *
  * position(haystack, needle)    - обычный поиск подстроки в строке, возвращает позицию (в байтах) найденной подстроки, начиная с 1, или 0, если подстрока не найдена.
  * positionUTF8(haystack, needle) - то же самое, но позиция вычисляется в кодовых точках, при условии, что строка в кодировке UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, pattern)        - поиск по регулярному выражению LIKE; возвращает 0 или 1. Регистронезависимое, но только для латиницы.
  * notLike(haystack, pattern)
  *
  * match(haystack, pattern)    - поиск по регулярному выражению re2; возвращает 0 или 1.
  *
  * Применяет регексп re2 и достаёт:
  * - первый subpattern, если в regexp-е есть subpattern;
  * - нулевой subpattern (сматчившуюся часть, иначе);
  * - если не сматчилось - пустую строку.
  * extract(haystack, pattern)
  *
  * replaceOne(haystack, pattern, replacement) - замена шаблона по заданным правилам, только первое вхождение.
  * replaceAll(haystack, pattern, replacement) - замена шаблона по заданным правилам, все вхождения.
  *
  * replaceRegexpOne(haystack, pattern, replacement) - замена шаблона по заданному регекспу, только первое вхождение.
  * replaceRegexpAll(haystack, pattern, replacement) - замена шаблона по заданному регекспу, все вхождения.
  *
  * Внимание! На данный момент, аргументы needle, pattern, n, replacement обязаны быть константами.
  */


template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionsStringSearch>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.safeGetByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.safeGetByPosition(arguments[1]).column;

        const ColumnConstString * col_haystack_const = typeid_cast<const ColumnConstString *>(&*column_haystack);
        const ColumnConstString * col_needle_const = typeid_cast<const ColumnConstString *>(&*column_needle);

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            Impl::constant_constant(col_haystack_const->getData(), col_needle_const->getData(), res);
            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<ResultType>>(col_haystack_const->size(), res);
            return;
        }

        auto col_res = std::make_shared<ColumnVector<ResultType>>();
        block.safeGetByPosition(result).column = col_res;

        typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = typeid_cast<const ColumnString *>(&*column_haystack);
        const ColumnString * col_needle_vector = typeid_cast<const ColumnString *>(&*column_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vector_vector(col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                vec_res);
        else if (col_haystack_vector && col_needle_const)
            Impl::vector_constant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), col_needle_const->getData(), vec_res);
        else if (col_haystack_const && col_needle_vector)
            Impl::constant_vector(col_haystack_const->getData(), col_needle_vector->getChars(), col_needle_vector->getOffsets(), vec_res);
        else
            throw Exception("Illegal columns " + block.safeGetByPosition(arguments[0]).column->getName() + " and "
                    + block.safeGetByPosition(arguments[1]).column->getName()
                    + " of arguments of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename Impl, typename Name>
class FunctionsStringSearchToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionsStringSearchToString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.safeGetByPosition(arguments[1]).column;

        const ColumnConstString * col_needle = typeid_cast<const ColumnConstString *>(&*column_needle);
        if (!col_needle)
            throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

        if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
        {
            std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_res;

            ColumnString::Chars_t & vec_res = col_res->getChars();
            ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), col_needle->getData(), vec_res, offsets_res);
        }
        else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
        {
            const std::string & data = col->getData();
            ColumnString::Chars_t vdata(reinterpret_cast<const ColumnString::Chars_t::value_type *>(data.c_str()),
                reinterpret_cast<const ColumnString::Chars_t::value_type *>(data.c_str() + data.size() + 1));
            ColumnString::Offsets_t offsets(1, vdata.size());
            ColumnString::Chars_t res_vdata;
            ColumnString::Offsets_t res_offsets;
            Impl::vector(vdata, offsets, col_needle->getData(), res_vdata, res_offsets);

            std::string res;

            if (!res_offsets.empty())
                res.assign(&res_vdata[0], &res_vdata[res_vdata.size() - 1]);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col->size(), res);
        }
        else
            throw Exception(
                "Illegal column " + block.safeGetByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
