#pragma once

#include <Parsers/ASTDataType.h>


namespace DB
{

/// Specialized AST for Tuple data types with named elements.
/// Stores element names directly as a vector of strings instead of creating
/// ASTNameTypePair children, significantly reducing memory for named tuples.
///
/// For named tuples: element_names[i] corresponds to arguments->children[i]
/// For unnamed tuples: element_names is empty, arguments->children contains types
class ASTTupleDataType : public ASTDataType
{
public:
    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    /// Set element names for named tuple. Names vector must either be:
    /// - Empty (unnamed tuple)
    /// - Same size as arguments->children (named tuple, all names must be non-empty)
    void setElementNames(Strings names)
    {
        element_names = std::move(names);
    }

    const Strings & getElementNames() const { return element_names; }
    bool hasNames() const { return !element_names.empty(); }

protected:
    /// Outputs: Tuple(name1 Type1, name2 Type2, ...) for named
    ///          Tuple(Type1, Type2, ...) for unnamed
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    /// Element names for named tuple.
    /// If empty, it's an unnamed tuple.
    /// If non-empty, must have same size as arguments->children, all names must be non-empty.
    Strings element_names;
};

}
