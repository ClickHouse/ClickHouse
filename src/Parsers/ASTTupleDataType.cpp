#include <Parsers/ASTTupleDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

String ASTTupleDataType::getID(char delim) const
{
    return "TupleDataType" + (delim + name);
}

ASTPtr ASTTupleDataType::clone() const
{
    auto res = std::make_shared<ASTTupleDataType>(*this);
    res->children.clear();

    if (arguments)
    {
        res->arguments = arguments->clone();
        res->children.push_back(res->arguments);
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
    if (arguments)
        arguments->updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTTupleDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << name;

    if (arguments && !arguments->children.empty())
    {
        ostr << '(';

        bool has_names = !element_names.empty();

        for (size_t i = 0; i < arguments->children.size(); ++i)
        {
            if (i > 0)
                ostr << ", ";

            /// Print element name if present and non-empty
            if (has_names && i < element_names.size() && !element_names[i].empty())
            {
                ostr << element_names[i] << ' ';
            }

            /// Print the type
            arguments->children[i]->format(ostr, settings, state, frame);
        }

        ostr << ')';
    }
}

}
