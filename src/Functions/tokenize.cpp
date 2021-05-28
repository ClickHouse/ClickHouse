#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionFactory.h>
//#include <Poco/Unicode.h>


#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <boost/tokenizer.hpp>
//#include <string_view>
#include <boost/range/iterator_range.hpp>

//Debug


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNSUPPORTED_LANGUAGE;
}

class FunctionTokenize : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = "tokenize";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTokenize>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    // Is it?
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & strcolumn = arguments[0].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(strcolumn.get())) 
        {
            /// Create and fill the result array.
            const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

            auto out = ColumnArray::create(elem_type->createColumn());
            IColumn & out_data = out->getData();
            IColumn::Offsets & out_offsets = out->getOffsets();

            const ColumnString::Chars & data = col->getChars();
            const ColumnString::Offsets & offsets = col->getOffsets();
            out_data.reserve(input_rows_count);
            out_offsets.resize(input_rows_count);

            IColumn::Offset current_offset = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                /// Doing boost::tokenize
                UInt64 num_elements = 0;
                boost::iterator_range<const UInt8*> text(data.data() + offsets[i - 1],
                                                         data.data() + offsets[i] - 1);
                boost::tokenizer<boost::char_delimiters_separator<UInt8>, const UInt8 *> tokenizer(text);
                for (const auto &token : tokenizer) {
                    out_data.insert(Field(token.data(), token.length()));
                    num_elements++;
                }
                
                current_offset += num_elements;
                out_offsets[i] = current_offset;
            }

            return out;
        }

        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionTokenize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTokenize>(FunctionFactory::CaseInsensitive);
}

}
