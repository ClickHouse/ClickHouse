#include <DataTypes/SubcolumnCodecs.h>

#include <Compression/CompressionFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTupleDataType.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Type wrappers through which a tuple element keeps its subcolumn name,
/// e.g. the subcolumn name of `x` in `Array(Tuple(x UInt64))` is just "x".
bool isTransparentTypeWrapper(const String & type_name)
{
    return type_name == "Array"
        || type_name == "Nullable"
        || type_name == "LowCardinality"
        || type_name == "SimpleAggregateFunction"
        || type_name == "AggregateFunction";
}

/// Calls `visit_element(tuple, element_index, subcolumn_name)` for every named tuple element in the type AST.
/// `subcolumn_name` is the full dotted path of the element relative to the column.
template <typename Visitor>
void visitTupleElements(IAST & ast, const String & prefix, const Visitor & visit_element)
{
    if (auto * tuple = ast.as<ASTTupleDataType>())
    {
        const auto arguments = tuple->getArguments();
        if (!arguments)
            return;

        const auto & argument_children = arguments->as<ASTExpressionList &>().children;
        for (size_t i = 0; i < argument_children.size(); ++i)
        {
            const String & element_name = (i < tuple->element_names.size()) ? tuple->element_names[i] : "";

            if (element_name.empty())
            {
                if (i < tuple->element_codecs.size() && tuple->element_codecs[i])
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codecs can be specified only for named Tuple elements");
            }
            else
            {
                visit_element(*tuple, i, prefix + element_name);
            }

            /// Elements of an unnamed tuple are addressed by their 1-based positions.
            String child_prefix = prefix + (element_name.empty() ? std::to_string(i + 1) : element_name) + ".";
            visitTupleElements(*argument_children[i], child_prefix, visit_element);
        }
        return;
    }

    if (const auto * data_type = ast.as<ASTDataType>())
    {
        const auto arguments = data_type->getArguments();
        if (!arguments)
            return;

        auto & argument_children = arguments->as<ASTExpressionList &>().children;

        if (data_type->name == "Map" && argument_children.size() == 2)
        {
            visitTupleElements(*argument_children[0], prefix + "keys.", visit_element);
            visitTupleElements(*argument_children[1], prefix + "values.", visit_element);
            return;
        }

        if (isTransparentTypeWrapper(data_type->name))
        {
            for (const auto & child : argument_children)
                visitTupleElements(*child, prefix, visit_element);
            return;
        }

        /// Codecs are not supported inside other types (Nested, Variant, JSON, ...) because
        /// there is no way to map a tuple element to a subcolumn there.
        for (const auto & child : argument_children)
        {
            if (child && typeASTHasSubcolumnCodecs(*child))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codecs for Tuple elements are not supported inside type {}", data_type->name);
        }
    }
}

void clearEmptyElementCodecs(IAST & ast)
{
    if (auto * tuple = ast.as<ASTTupleDataType>())
    {
        bool all_empty = true;
        for (const auto & codec : tuple->element_codecs)
        {
            if (codec)
            {
                all_empty = false;
                break;
            }
        }
        if (all_empty)
            tuple->element_codecs.clear();
    }

    for (const auto & child : ast.children)
        clearEmptyElementCodecs(*child);
}

}

bool typeASTHasSubcolumnCodecs(const IAST & type_ast)
{
    if (const auto * tuple = type_ast.as<ASTTupleDataType>())
    {
        for (const auto & codec : tuple->element_codecs)
            if (codec)
                return true;
    }

    for (const auto & child : type_ast.children)
        if (child && typeASTHasSubcolumnCodecs(*child))
            return true;

    return false;
}

ASTPtr extractSubcolumnCodecsFromTypeAST(const ASTPtr & type_ast, SubcolumnCodecs & out_codecs)
{
    if (!typeASTHasSubcolumnCodecs(*type_ast))
        return type_ast;

    ASTPtr cleaned = type_ast->clone();

    visitTupleElements(*cleaned, "", [&](ASTTupleDataType & tuple, size_t element_index, const String & subcolumn_name)
    {
        if (element_index >= tuple.element_codecs.size() || !tuple.element_codecs[element_index])
            return;

        if (!out_codecs.emplace(subcolumn_name, std::move(tuple.element_codecs[element_index])).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple codecs are specified for subcolumn {}", subcolumn_name);

        tuple.element_codecs[element_index] = nullptr;
    });

    clearEmptyElementCodecs(*cleaned);
    return cleaned;
}

ASTPtr typeASTWithoutSubcolumnCodecs(const ASTPtr & type_ast)
{
    SubcolumnCodecs ignored_codecs;
    return extractSubcolumnCodecsFromTypeAST(type_ast, ignored_codecs);
}

void injectSubcolumnCodecsIntoTypeAST(IAST & type_ast, const SubcolumnCodecs & codecs)
{
    if (codecs.empty())
        return;

    size_t num_injected = 0;

    visitTupleElements(type_ast, "", [&](ASTTupleDataType & tuple, size_t element_index, const String & subcolumn_name)
    {
        auto it = codecs.find(subcolumn_name);
        if (it == codecs.end())
            return;

        if (tuple.element_codecs.empty())
            tuple.element_codecs.resize(tuple.getArguments()->as<ASTExpressionList &>().children.size());

        tuple.element_codecs[element_index] = it->second->clone();
        ++num_injected;
    });

    if (num_injected != codecs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot put codecs of subcolumns back into the type AST {}: only {} of {} codecs matched tuple elements",
            type_ast.formatForErrorMessage(), num_injected, codecs.size());
}

String getTypeNameWithSubcolumnCodecs(const DataTypePtr & type, const SubcolumnCodecs & codecs)
{
    if (codecs.empty())
        return type->getName();

    auto type_ast = dataTypeToAST(type);
    injectSubcolumnCodecsIntoTypeAST(*type_ast, codecs);
    return type_ast->formatWithSecretsOneLine();
}

void validateSubcolumnCodecs(
    const String & column_name,
    const DataTypePtr & column_type,
    SubcolumnCodecs & codecs,
    const CodecValidationSettings & validation_settings)
{
    for (auto & [subcolumn_name, codec] : codecs)
    {
        auto subcolumn_type = column_type->tryGetSubcolumnType(subcolumn_name);
        if (!subcolumn_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot specify codec for subcolumn {}: there is no such subcolumn in column {} of type {}",
                subcolumn_name, column_name, column_type->getName());

        codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, subcolumn_type, validation_settings);
    }
}

SubcolumnCodecs cloneSubcolumnCodecs(const SubcolumnCodecs & codecs)
{
    SubcolumnCodecs res;
    for (const auto & [subcolumn_name, codec] : codecs)
        res.emplace(subcolumn_name, codec->clone());
    return res;
}

}
