#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
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

    const auto & arguments = getArguments();
    if (arguments)
    {
        res->children.emplace_back(arguments->clone());
    }

    /// element_names vector is copied by the copy constructor
    return res;
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

    /// Hash child types via arguments
    if (const auto arguments = getArguments())
        arguments->updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTTupleDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto arguments = getArguments();
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

                /// Print element name if present
                if (i < element_names.size())
                    ostr << backQuoteIfNeed(element_names[i]) << ' ';

                /// Print the type
                arguments->children[i]->format(ostr, settings, state, frame);
            }
        }
        else
        {
            /// Single-line format (for single-element tuples or when one_line is true)
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i > 0)
                    ostr << ", ";

                /// Print element name if present
                if (i < element_names.size())
                    ostr << backQuoteIfNeed(element_names[i]) << ' ';

                /// Print the type
                arguments->children[i]->format(ostr, settings, state, frame);
            }
        }

        ostr << ')';
    }
}

void ASTTupleDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TupleDataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);

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

    auto args = r.readChildOfType<ASTExpressionList>("arguments");
    if (args)
        children.push_back(args);

    element_names = r.readStringArray("element_names");

    /// A named tuple names every element (mirrors `DataTypeFactory::createTupleFromAST`); reject a
    /// partial/oversized or empty-named list that the parser could never produce.
    if (!element_names.empty())
    {
        const size_t num_args = args ? args->children.size() : 0;
        if (element_names.size() != num_args)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ASTTupleDataType has {} element names but {} element types during AST JSON deserialization",
                element_names.size(), num_args);
        for (const auto & elem_name : element_names)
            if (elem_name.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTupleDataType element name must not be empty during AST JSON deserialization");
    }
}

}
