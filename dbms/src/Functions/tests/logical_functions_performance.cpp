#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunction.h>
#include <Common/Stopwatch.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


template <typename B>
struct AndImpl
{
    static inline UInt8 apply(UInt8 a, B b)
    {
        return a && b;
    }
};

template <typename B>
struct OrImpl
{
    static inline UInt8 apply(UInt8 a, B b)
    {
        return a || b;
    }
};

template <typename B>
struct XorImpl
{
    static inline UInt8 apply(UInt8 a, B b)
    {
        return (!a) != (!b);
    }
};


using UInt8Container = ColumnUInt8::Container_t;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8 *>;

template <typename Op, size_t N>
struct AssociativeOperationImpl
{
    /// Throws the last N columns from `in` (if there are less, then all) and puts their combination into `result`.
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

    /// Remembers the last N columns from in.
    explicit AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(in[in.size() - N]->getData()), continuation(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline UInt8 apply(size_t i) const
    {
        //return vec[i] ? continuation.apply(i) : 0;
        return Op::apply(vec[i], continuation.apply(i));
        //return vec[i] && continuation.apply(i);
    }
};

template <typename Op>
struct AssociativeOperationImpl<Op, 1>
{
    static void execute(UInt8ColumnPtrs & in, UInt8Container & result)
    {
        throw Exception("Logical error: AssociativeOperationImpl<Op, 1>::execute called", ErrorCodes::LOGICAL_ERROR);
    }

    const UInt8 * vec;

    explicit AssociativeOperationImpl(UInt8ColumnPtrs & in)
        : vec(&in[in.size() - 1]->getData()[0]) {}

    inline UInt8 apply(size_t i) const
    {
        return vec[i];
    }
};


template <template <typename> class Impl, typename Name>
class FunctionAnyArityLogical : public IFunction
{
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
        return Name::get();
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnType(const DataTypes & arguments) const
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

    void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        //Stopwatch sw;
        ColumnPlainPtrs in(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            in[i] = block.getByPosition(arguments[i]).column.get();
        }
        size_t n = in[0]->size();

        /// Combine all constant columns into a single value.
        UInt8 const_val = 0;
        bool has_consts = extractConstColumns(in, const_val);

        // If this value uniquely determines the result, return it.
        if (has_consts && (in.empty() || Impl<UInt8>::apply(const_val, 0) == Impl<UInt8>::apply(const_val, 1)))
        {
            if (!in.empty())
                const_val = Impl<UInt8>::apply(const_val, 0);
            auto col_res = DataTypeUInt8().createConstColumn(n, const_val);
            block.getByPosition(result).column = col_res;
            return;
        }

        /// If this value is a neutral element, let's forget about it.
        if (has_consts && Impl<UInt8>::apply(const_val, 0) == 0 && Impl<UInt8>::apply(const_val, 1) == 1)
            has_consts = false;

        auto col_res = std::make_shared<ColumnUInt8>();
        block.getByPosition(result).column = col_res;
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

        /// Divide the input columns into UInt8 and the rest. The first will be processed more efficiently.
        /// col_res at each moment will either be at the end of uint8_in, or not contained in uint8_in.
        UInt8ColumnPtrs uint8_in;
        ColumnPlainPtrs other_in;
        for (IColumn * column : in)
        {
            if (auto uint8_column = typeid_cast<const ColumnUInt8 *>(column))
                uint8_in.push_back(uint8_column);
            else
                other_in.push_back(column);
        }

        /// You need at least one column in uint8_in, so that you can combine columns with other_in.
        if (uint8_in.empty())
        {
            if (other_in.empty())
                throw Exception("Hello, I'm a bug", ErrorCodes::LOGICAL_ERROR);

            convertToUInt8(other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in.push_back(col_res.get());
        }

        /// Effectively combine all the columns of the correct type.
        while (uint8_in.size() > 1)
        {
            AssociativeOperationImpl<Impl<UInt8>, 10>::execute(uint8_in, vec_res);
            uint8_in.push_back(col_res.get());
        }

        /// One by one, add all the columns of the wrong type.
        while (!other_in.empty())
        {
            executeUInt8Other(uint8_in[0]->getData(), other_in.back(), vec_res);
            other_in.pop_back();
            uint8_in[0] = col_res.get();
        }

        /// This is possible if there are exactly one non-constant among the arguments, and it is of type UInt8.
        if (uint8_in[0] != col_res.get())
        {
            vec_res.assign(uint8_in[0]->getData());
        }

        //double seconds = sw.elapsedSeconds();
        //std::cerr << seconds << " seconds" << std::endl;
    }
};


struct NameAnd { static const char * get() { return "and"; } };
struct NameOr { static const char * get() { return "or"; } };
struct NameXor { static const char * get() { return "xor"; } };

using FunctionAnd = FunctionAnyArityLogical<AndImpl, NameAnd>;
using FunctionOr = FunctionAnyArityLogical<OrImpl, NameOr>    ;
using FunctionXor = FunctionAnyArityLogical<XorImpl, NameXor>;
}

using namespace DB;


int main(int argc, char ** argv)
{
    try
    {
        size_t block_size = 1ULL << 20;
        if (argc > 1)
        {
            block_size = atoi(argv[1]);
            if (block_size < 50)
                block_size = 1l << block_size;
        }
        size_t block_count = (100000000 - 1) / block_size + 1;
        size_t columns = 10;
        size_t repeats = 3;

        std::vector<Block> blocks(block_count);

        for (size_t b = 0; b < block_count; ++b)
        {
            for (size_t i = 0; i < columns; ++i)
            {
                auto column = std::make_shared<ColumnUInt8>(block_size);
                blocks[b].insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeUInt8>(), "v" + toString(i)));

                ColumnUInt8::Container_t & vec = column->getData();
                vec.resize(block_size);

                for (size_t j = 0; j < block_size; ++j)
                {
                    vec[j] = rand() % 2;
                }
            }
        }
        for (size_t b = 0; b < block_count; ++b)
        {
            auto result_column = std::make_shared<ColumnUInt8>();
            blocks[b].insert(ColumnWithTypeAndName(result_column, std::make_shared<DataTypeUInt8>(), "x"));
            result_column->getData().resize(block_size);
        }

        for (size_t arity = 2; arity <= columns; ++arity)
        {
            FunctionPtr function = std::make_shared<FunctionAnd>();
            function->getReturnType(DataTypes(arity, DataTypePtr(std::make_shared<DataTypeUInt8>())));

            ColumnNumbers arguments(arity);
            for (size_t i = 0; i < arity; ++i)
            {
                arguments[i] = i;
            }

            std::cerr << arity << "-ary:" << std::endl;

            Stopwatch outer_sw;
            for (size_t i = 0; i < repeats; ++i)
            {
                Stopwatch inner_sw;
                for (size_t b = 0; b < block_count; ++b)
                {
                    function->execute(blocks[b], arguments, columns);
                }
                std::cerr << "iteration " << i + 1 << ": " << std::setprecision(3) << inner_sw.elapsedSeconds() << " seconds." << std::endl;
            }
            double seconds = outer_sw.elapsedSeconds();
            std::cout << arity << "-ary average: " << std::setprecision(3) << seconds / repeats << " seconds, " << seconds / repeats / (arity - 1) << " seconds per operation." << std::endl;
        }
    }
    catch (Exception & e)
    {
        std::cerr << "exception" << std::endl;
    }

    return 0;
}
