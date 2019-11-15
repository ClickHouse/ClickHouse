#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>

#include <random>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class FunctionRandomASKII : public IFunction
{

public:
    static constexpr auto name = "randomASKII";
    static FunctionPtr create(const Context &){ return std::make_shared<FunctionRandomASKII>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (!(executeType<UInt8>(block, arguments, result)
            || executeType<UInt16>(block, arguments, result)
            || executeType<UInt32>(block, arguments, result)
            || executeType<UInt64>(block, arguments, result)
            || executeType<Int8>(block, arguments, result)
            || executeType<Int16>(block, arguments, result)
            || executeType<Int32>(block, arguments, result)
            || executeType<Int64>(block, arguments, result)
            || executeType<Float32>(block, arguments, result)
            || executeType<Float64>(block, arguments, result)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);


	    char charachter;
	    size_t ch_num = 0;

            for (size_t i = 0; i < size; ++i)
            {

		std::default_random_engine generator(i);
		std::uniform_int_distribution<int> distribution(32, 127);

		while( ch_num < static_cast<size_t>(vec_from[i])){
		    charachter = distribution(generator);
		    std::cout<<"==================="<<charachter<<std::endl;
		    writeChar(charachter, buf_to);
		    ch_num++;
		}


//		for (size_t ch_num = 32; ch_num < 45 ; ++ch_num)
//		{
//		    writeChar(ch_num, buf_to);
//		}
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }

            buf_to.finish();
            block.getByPosition(result).column = std::move(col_to);

//            block.getByPosition(result).column = DataTypeString().createColumnConst(col_from->size(), "randomASKII");
            return true;
        }

        return false;
    }



    // explicit FunctionRandomASKII()
    // {
    // }

    // size_t getNumberOfArguments() const override
    // {
    //     return 0;
    // }

    // DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    // {
    //     return std::make_shared<DataTypeString>();
    // }

    // bool isDeterministic() const override { return false; }

    // void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    // {
    //     block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, "randomASKII");
    // }
};


void registerFunctionRandomASKII(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomASKII>();
}

}
