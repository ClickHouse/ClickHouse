#include <Parsers/ASTDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

String ASTDataType::getID(char delim) const
{
    return "DataType" + (delim + name);
}

ASTPtr ASTDataType::clone() const
{
    auto res = std::make_shared<ASTDataType>(*this);
    res->children.clear();

    if (arguments)
    {
        res->arguments = arguments->clone();
        res->children.push_back(res->arguments);
    }

    return res;
}

void ASTDataType::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    /// Children are hashed automatically.
}

void ASTDataType::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_function : "") << name;

    if (arguments && !arguments->children.empty())
    {
        settings.ostr << '(' << (settings.hilite ? hilite_none : "");

        for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << ", ";

            arguments->children[i]->formatImpl(settings, state, frame);
        }

        settings.ostr << (settings.hilite ? hilite_function : "") << ')';
    }

    settings.ostr << (settings.hilite ? hilite_none : "");
}

}
