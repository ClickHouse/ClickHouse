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
    /// Element names for named tuple (empty string means unnamed element)
    /// If element_names is empty, it's a purely unnamed tuple
    Strings element_names;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    /// Outputs: Tuple(name1 Type1, name2 Type2, ...) for named
    ///          Tuple(Type1, Type2, ...) for unnamed
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
