#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTTupleElementCodecOperation.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String ASTTupleDataType::getID(char delim) const
{
    return "TupleDataType" + (delim + name);
}

ASTPtr ASTTupleDataType::clone() const
{
    auto res = make_intrusive<ASTTupleDataType>(*this);
    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());
    return res;
}

ASTPtr ASTTupleDataType::getCodecOperations() const
{
    if (children.size() < 2)
        return nullptr;
    return children[1];
}

ASTTupleDataType::CodecOperationsByElement ASTTupleDataType::getCodecOperationsByElement() const
{
    validateCodecOperations();
    const auto operations = getCodecOperations();
    if (!operations)
        return {};

    const auto arguments = getArguments();
    CodecOperationsByElement result(arguments ? arguments->children.size() : 0, nullptr);
    for (const auto & child : operations->children)
    {
        const auto & operation = child->as<ASTTupleElementCodecOperation &>();
        result[operation.element_index] = &operation;
    }
    return result;
}

void ASTTupleDataType::setCodecOperations(ASTs codec_operations)
{
    validateCodecOperations();
    if (codec_operations.empty())
    {
        if (getCodecOperations())
            children.erase(children.begin() + 1);
        return;
    }

    auto operations = make_intrusive<ASTExpressionList>();
    operations->children = std::move(codec_operations);
    if (getCodecOperations())
        children[1] = std::move(operations);
    else
        children.push_back(std::move(operations));
    validateCodecOperations();
}

void ASTTupleDataType::resetCodecOperations()
{
    validateCodecOperations();
    if (getCodecOperations())
        children.erase(children.begin() + 1);
}

void ASTTupleDataType::validateCodecOperations() const
{
    if (children.size() > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple data type AST contains unexpected children");
    if (!children.empty() && !children.front()->as<ASTExpressionList>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple data type arguments must be an expression list");
    if (children.size() == 2 && !children[1]->as<ASTExpressionList>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple CODEC operations must be an expression list");

    const auto arguments = getArguments();
    const size_t argument_count = arguments ? arguments->children.size() : 0;
    const auto operations = getCodecOperations();
    if (!operations)
        return;

    std::vector<bool> seen(argument_count);
    for (const auto & child : operations->children)
    {
        const auto * operation = child->as<ASTTupleElementCodecOperation>();
        if (!operation)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple CODEC operation list contains an unexpected AST node");
        operation->validate();
        if (operation->element_index >= argument_count)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Tuple CODEC operation index {} is out of range for {} elements",
                operation->element_index,
                argument_count);
        if (seen[operation->element_index])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate CODEC operation for Tuple element {}", operation->element_index);
        seen[operation->element_index] = true;
    }
}

void ASTTupleDataType::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    /// Hash element names
    hash_state.update(element_names.size());
    for (const auto & elem_name : element_names)
    {
        hash_state.update(elem_name.size());
        hash_state.update(elem_name);
    }

    (void)ignore_aliases;
    /// Arguments and sparse codec operations are ordinary children and are hashed automatically.
}

void ASTTupleDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto arguments = getArguments();
    const auto codec_operations = getCodecOperationsByElement();
    const auto get_codec_operation = [&](size_t index)
    {
        return codec_operations.empty() ? nullptr : codec_operations[index];
    };
    ostr << name;

    if (arguments && !arguments->children.empty())
    {
        ostr << '(';

        /// Pretty print with newlines only for tuples with more than 1 column
        bool use_multiline = !settings.one_line && settings.print_pretty_type_names && arguments->children.size() > 1;

        if (use_multiline && element_names.empty())
        {
            /// Unnamed tuple with multiple elements - use multiline format
            ++frame.indent;
            std::string indent_str = "\n" + std::string(4 * frame.indent, ' ');
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    ostr << ',';
                ostr << indent_str;
                arguments->children[i]->format(ostr, settings, state, frame);
                if (const auto * operation = get_codec_operation(i))
                {
                    ostr << ' ';
                    operation->format(ostr, settings, state, frame);
                }
            }
        }
        else if (use_multiline && !element_names.empty())
        {
            /// Named tuple with multiple elements - use multiline format
            ++frame.indent;
            std::string indent_str = "\n" + std::string(4 * frame.indent, ' ');
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    ostr << ',';
                ostr << indent_str;

                /// An unnamed element is represented by an empty name, which has
                /// nothing to serialize, so skip it.
                if (i < element_names.size() && !element_names[i].empty())
                    ostr << backQuoteIfNeed(element_names[i]) << ' ';

                /// Print the type
                arguments->children[i]->format(ostr, settings, state, frame);
                if (const auto * operation = get_codec_operation(i))
                {
                    ostr << ' ';
                    operation->format(ostr, settings, state, frame);
                }
            }
        }
        else
        {
            /// Single-line format (for single-element tuples or when one_line is true)
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i > 0)
                    ostr << ", ";

                /// As in the multiline branch above, skip an unnamed (empty-name) element.
                if (i < element_names.size() && !element_names[i].empty())
                    ostr << backQuoteIfNeed(element_names[i]) << ' ';

                /// Print the type
                arguments->children[i]->format(ostr, settings, state, frame);
                if (const auto * operation = get_codec_operation(i))
                {
                    ostr << ' ';
                    operation->format(ostr, settings, state, frame);
                }
            }
        }

        ostr << ')';
    }

}

void ASTTupleDataType::writeJSON(WriteBuffer & out) const
{
    validateCodecOperations();
    JSONObjectWriter w(out, "TupleDataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);
    if (auto operations = getCodecOperations())
        w.writeChild("codec_operations", operations);

    /// Named-tuple field names live in `element_names`, not as AST children, so write them explicitly;
    /// the generic `ASTDataType::writeJSON` would drop them (turning `Tuple(a UInt8)` into `Tuple(UInt8)`).
    if (!element_names.empty())
    {
        w.writeKey("element_names");
        auto & o = w.getOut();
        o << '[';
        for (size_t i = 0; i < element_names.size(); ++i)
        {
            if (i > 0)
                o << ',';
            writeJSONString(element_names[i], o, w.getFormatSettings());
        }
        o << ']';
    }
}

void ASTTupleDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for ASTTupleDataType during AST JSON deserialization");

    children.clear();
    auto args = r.readChildOfType<ASTExpressionList>("arguments");
    if (args)
    {
        for (const auto & argument : args->children)
            if (!argument || !dynamic_cast<const ASTDataType *>(argument.get()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "ASTTupleDataType element type must be an ASTDataType during AST JSON deserialization");
        children.push_back(args);
    }
    if (auto operations = r.readChildOfType<ASTExpressionList>("codec_operations"))
    {
        if (!args)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tuple CODEC operations require Tuple element types");
        children.push_back(std::move(operations));
    }
    element_names = r.readStringArray("element_names");
    const size_t argument_count = args ? args->children.size() : 0;

    /// A named tuple names every element (mirrors `DataTypeFactory::createTupleFromAST`); reject a
    /// partial/oversized or empty-named list that the parser could never produce.
    if (!element_names.empty())
    {
        if (element_names.size() != argument_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ASTTupleDataType has {} element names but {} element types during AST JSON deserialization",
                element_names.size(), argument_count);
        for (const auto & elem_name : element_names)
            if (elem_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTupleDataType element name must not be empty during AST JSON deserialization");
    }

    validateCodecOperations();
}

}
