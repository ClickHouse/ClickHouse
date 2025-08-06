#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/XPath/Instructions/InstructionApplier.h>
#include <Functions/XPath/Instructions/InstructionParser.h>
#include <Functions/XPath/Parsers/ParserXPath.h>
#include <Functions/XPath/XMLTagScanner.h>

#include <Parsers/IParser.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionExtractXML : public IFunction
{
public:
    static constexpr auto name = "extractXML";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractXML>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        const auto & xml_column = arguments[0];
        const auto & xpath_column = arguments[1];

        if (!isColumnConst(*xpath_column.column))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument (XPath) must be constant string");
        }

        String xpath_query = typeid_cast<const ColumnConst &>(*xpath_column.column).getValue<String>();
        Tokens tokens(xpath_query.data(), xpath_query.data() + xpath_query.size());
        IParser::Pos token_iterator(tokens, 0, 1000);

        ParserXPath xpath_parser;
        Expected expected;
        ASTPtr res;
        const bool parse_res = xpath_parser.parse(token_iterator, res, expected);
        if (!parse_res)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse XPath");
        }

        const auto instructions = getInstructionsFromAST(res);

        auto col_res = ColumnArray::create(ColumnString::create());

        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnArray::Offsets & res_offsets = col_res->getOffsets();
        ColumnArray::Offset current_offset = 0;

        TagScannerPtr tag_scanner = std::make_shared<XMLTagScanner>();

        for (size_t i = 0; i < rows; ++i)
        {
            std::string_view xml_document = xml_column.column->getDataAt(i).toView();
            std::vector<Document> result
                = applyInstructions(xml_document.data(), xml_document.data() + xml_document.size(), instructions, tag_scanner);

            for (const auto & doc : result)
            {
                res_strings.insertData(doc.begin, doc.end - doc.begin);
            }

            current_offset += result.size();
            res_offsets.push_back(current_offset);
        }

        return col_res;
    }
};

REGISTER_FUNCTION(ExtractXML)
{
    factory.registerFunction<FunctionExtractXML>();
}

}
