#pragma once

#include <Core/Types.h>
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
    /// Element names for named tuple.
    /// If empty, it's an unnamed tuple.
    /// If non-empty, must have same size as arguments->children, all names must be non-empty.
    /// Validation happens in DataTypeFactory::createTupleFromAST().
    Strings element_names;

    /// Optional codecs of elements: Tuple(x UInt64 CODEC(Delta, ZSTD), y String).
    /// If non-empty, has the same size as arguments->children; an entry is nullptr for an element
    /// without a codec. Codecs can be specified only for named elements.
    /// A codec is not a part of the data type: such type declarations are allowed only for columns
    /// of CREATE TABLE and ALTER TABLE queries, where the codecs are extracted from the type AST
    /// before the type is created (see extractSubcolumnCodecsFromTypeAST()).
    ASTs element_codecs;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    /// Named-tuple field names live in `element_names` (not as AST children), so the generic
    /// `ASTDataType` JSON serialization would drop them. Serialize/restore them under a distinct tag.
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    /// Outputs: Tuple(name1 Type1, name2 Type2, ...) for named
    ///          Tuple(Type1, Type2, ...) for unnamed
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
