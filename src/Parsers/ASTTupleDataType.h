#pragma once

#include <Core/Types.h>
#include <Parsers/ASTDataType.h>

namespace DB
{

class ASTTupleElementCodecOperation;

/// Specialized AST for Tuple data types with named elements.
/// Stores element names directly as a vector of strings instead of creating
/// ASTNameTypePair children, significantly reducing memory for named tuples.
///
/// For named tuples: element_names[i] corresponds to arguments->children[i]
/// For unnamed tuples: element_names is empty, arguments->children contains types
class ASTTupleDataType : public ASTDataType
{
public:
    using CodecOperationsByElement = std::vector<const ASTTupleElementCodecOperation *>;

    /// Element names for named tuple.
    /// If empty, it's an unnamed tuple.
    /// If non-empty, must have same size as arguments->children, all names must be non-empty.
    /// Validation happens in DataTypeFactory::createTupleFromAST().
    Strings element_names;

    ASTPtr getCodecOperations() const;
    /// Validate once and index sparse operations by Tuple element. Returns an empty vector when there are no operations.
    CodecOperationsByElement getCodecOperationsByElement() const;
    /// Replace all sparse operations and validate the resulting list once.
    void setCodecOperations(ASTs codec_operations);
    void resetCodecOperations();
    void validateCodecOperations() const;

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
