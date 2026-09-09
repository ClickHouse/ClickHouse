#include <Parsers/ASTTupleDataType.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

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

}
