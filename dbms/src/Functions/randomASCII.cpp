#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <type_traits>

#include <random>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class FunctionRandomASCII : public IFunction
{
public:
    static constexpr auto name = "randomASCII";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomASCII>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception("Cannot format " + type.getName() + " as size in bytes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        if (!(executeType<UInt8>(block, arguments, result, input_rows_count)
            || executeType<UInt16>(block, arguments, result, input_rows_count)
            || executeType<UInt32>(block, arguments, result, input_rows_count)
            || executeType<UInt64>(block, arguments, result, input_rows_count)
            || executeType<Int8>(block, arguments, result, input_rows_count)
            || executeType<Int16>(block, arguments, result, input_rows_count)
            || executeType<Int32>(block, arguments, result, input_rows_count)
            || executeType<Int64>(block, arguments, result, input_rows_count)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        bool is_const_column = false;
        const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get());

        if (!col_from){
            col_from = checkAndGetColumnConstData<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get());
            is_const_column = true;
        }
                
        if (col_from){

            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            offsets_to.resize(input_rows_count);

            WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

            std::default_random_engine generator;
            std::uniform_int_distribution<int> distribution(32, 127); //Printable ASCII symbols
            std::random_device rd;
            char character;
            size_t str_length = 0;

            if (is_const_column){
                str_length = static_cast<size_t>(vec_from[0]);                    
            }

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (!is_const_column){
                    str_length = static_cast<size_t>(vec_from[i]);
                }
                
                generator.seed( rd() );
                
                if (str_length > 0){
                    for (size_t j = 0; j < str_length; ++j)
                    {
                        character = distribution(generator);
                        writeChar(character, buf_to);
                    }
                }

                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }

            buf_to.finish();
            block.getByPosition(result).column = std::move(col_to);
            return true;
        }

        return false;
    }
};

void registerFunctionRandomASCII(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomASCII>();
}

}
