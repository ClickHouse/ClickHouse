#include <Functions/HTML/LxbDocument.h>

#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <lexbor/html/serialize.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_html_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

lxb_status_t serializeToChars(const lxb_char_t * data, size_t len, void * ctx)
{
    auto * chars = static_cast<ColumnString::Chars *>(ctx);
    chars->insert(chars->end(), data, data + len);
    return LXB_STATUS_OK;
}

}

class FunctionHTMLParse : public IFunction
{
public:
    static constexpr auto name = "htmlParse";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_html_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "HTML function '{}' is experimental. "
                "Set `allow_experimental_html_functions` setting to enable it", name);
        return std::make_shared<FunctionHTMLParse>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}", arguments[0].type->getName(), getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        auto result = ColumnString::create();
        auto & chars = result->getChars();
        auto & offsets = result->getOffsets();

        const size_t rows = col->size();
        LxbDocument lxb_doc;

        for (size_t i = 0; i < rows; ++i)
        {
            auto html = col->getDataAt(i);
            chars.reserve(chars.size() + html.size());

            if (html.size() == 0 || !lxb_doc)
            {
                chars.insert(chars.end(), html.data(), html.data() + html.size());
                offsets.push_back(chars.size());
                continue;
            }

            lxb_html_document_clean(lxb_doc.doc);
            lxb_status_t status = lxb_html_document_parse(lxb_doc.doc,
                reinterpret_cast<const lxb_char_t *>(html.data()), html.size());
            if (status != LXB_STATUS_OK)
            {
                chars.insert(chars.end(), html.data(), html.data() + html.size());
                offsets.push_back(chars.size());
                continue;
            }

            lxb_html_serialize_tree_cb(
                lxb_dom_interface_node(lxb_doc.doc),
                serializeToChars, &chars);
            offsets.push_back(chars.size());
        }
        return result;
    }
};

REGISTER_FUNCTION(HTMLParse)
{
    factory.registerFunction<FunctionHTMLParse>(FunctionDocumentation{
        .description = R"(Parses HTML and serializes it back. Normalizes malformed HTML into valid HTML5.)",
        .syntax = "htmlParse(html)",
        .arguments = {{"html", "HTML string to parse and normalize.", {"String"}}},
        .returned_value = {"Normalized HTML string.", {"String"}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::Other});
}

}
