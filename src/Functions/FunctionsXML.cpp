#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/XMLParsers/PocoXMLParser.h>
#include <Common/XMLParsers/XPathEvaluator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// XMLExtractString(xml, xpath) -> String
/// Extracts text content from XML using XPath expression.
/// Namespace prefixes are stripped during matching (e.g., 'Body' matches 'soap:Body').
/// Returns NULL if xpath doesn't match.
class FunctionXMLExtractString : public IFunction
{
public:
    static constexpr auto name = "XMLExtractString";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionXMLExtractString>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String (XML document)",
                arguments[0]->getName(),
                getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String (XPath expression)",
                arguments[1]->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * xml_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const ColumnString * xpath_col = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        if (!xml_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());
        if (!xpath_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be String", getName());

        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        result_col->reserve(input_rows_count);

        PocoXMLParser parser;
        XPathEvaluator evaluator;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view xml = xml_col->getDataAt(i).toView();
            std::string_view xpath = xpath_col->getDataAt(i).toView();

            bool found = false;
            if (parser.parse(xml))
            {
                auto root = parser.getRootNode();
                if (!root.isNull())
                {
                    std::string result = evaluator.evaluateToString(root, std::string(xpath));
                    if (!result.empty() || evaluator.evaluateToNode(root, std::string(xpath)).isNull() == false)
                    {
                        result_col->insertData(result.data(), result.size());
                        found = true;
                    }
                }
            }

            if (!found)
            {
                result_col->insertDefault();
                null_map->getData()[i] = 1;
            }
        }

        return ColumnNullable::create(std::move(result_col), std::move(null_map));
    }
};


/// XMLExtractRaw(xml, xpath) -> String
/// Extracts raw XML subtree from XML using XPath expression.
/// Returns NULL if xpath doesn't match.
class FunctionXMLExtractRaw : public IFunction
{
public:
    static constexpr auto name = "XMLExtractRaw";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionXMLExtractRaw>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String (XML document)",
                arguments[0]->getName(),
                getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String (XPath expression)",
                arguments[1]->getName(),
                getName());

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnString * xml_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const ColumnString * xpath_col = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        if (!xml_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be String", getName());
        if (!xpath_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be String", getName());

        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        result_col->reserve(input_rows_count);

        PocoXMLParser parser;
        XPathEvaluator evaluator;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view xml = xml_col->getDataAt(i).toView();
            std::string_view xpath = xpath_col->getDataAt(i).toView();

            bool found = false;
            if (parser.parse(xml))
            {
                auto root = parser.getRootNode();
                if (!root.isNull())
                {
                    auto node = evaluator.evaluateToNode(root, std::string(xpath));
                    if (!node.isNull())
                    {
                        std::string result = node.toXML();
                        result_col->insertData(result.data(), result.size());
                        found = true;
                    }
                }
            }

            if (!found)
            {
                result_col->insertDefault();
                null_map->getData()[i] = 1;
            }
        }

        return ColumnNullable::create(std::move(result_col), std::move(null_map));
    }
};

}

REGISTER_FUNCTION(XMLExtract)
{
    /// XMLExtractString
    {
        FunctionDocumentation::Description description = R"(
Extracts text content from an XML document using an XPath expression.

Namespace prefixes are stripped during element matching, so 'Body' matches 'soap:Body'.
This makes it easy to query SOAP messages without worrying about namespace prefixes.

Supports XPath 1.0 subset:
- Location paths: /, //, ., ..
- Node tests: element names, *, text(), node()
- Predicates: [n], [@attr], [@attr='value']
- Axes: child::, descendant::, parent::, attribute::, self::
)";
        FunctionDocumentation::Syntax syntax = "XMLExtractString(xml, xpath)";
        FunctionDocumentation::Arguments arguments = {
            {"xml", "XML document as a string", {"String"}},
            {"xpath", "XPath expression to evaluate", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The text content of the first matching node, or NULL if not found",
            {"Nullable(String)"}
        };
        FunctionDocumentation::Examples examples = {
            {"simple",
             "SELECT XMLExtractString('<root><item>Hello</item></root>', '/root/item')",
             "Hello"},
            {"namespace",
             "SELECT XMLExtractString('<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>Content</soap:Body></soap:Envelope>', '//Body')",
             "Content"},
            {"attribute",
             "SELECT XMLExtractString('<root><item id=\"1\">First</item><item id=\"2\">Second</item></root>', '/root/item[@id=\"2\"]')",
             "Second"}
        };
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, {}, category};

        factory.registerFunction<FunctionXMLExtractString>(documentation);
    }

    /// XMLExtractRaw
    {
        FunctionDocumentation::Description description = R"(
Extracts a raw XML subtree from an XML document using an XPath expression.

Unlike XMLExtractString which returns text content, this function returns the entire
XML fragment including tags. Namespace prefixes are stripped during element matching.
)";
        FunctionDocumentation::Syntax syntax = "XMLExtractRaw(xml, xpath)";
        FunctionDocumentation::Arguments arguments = {
            {"xml", "XML document as a string", {"String"}},
            {"xpath", "XPath expression to evaluate", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The serialized XML of the first matching node, or NULL if not found",
            {"Nullable(String)"}
        };
        FunctionDocumentation::Examples examples = {
            {"simple",
             "SELECT XMLExtractRaw('<root><item id=\"1\">Hello</item></root>', '/root/item')",
             "<item id=\"1\">Hello</item>"}
        };
        FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, {}, category};

        factory.registerFunction<FunctionXMLExtractRaw>(documentation);
    }
}

}
