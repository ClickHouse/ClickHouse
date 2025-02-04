#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

/// AST for single dictionary attribute in dictionary DDL query
class ASTDictionaryAttributeDeclaration : public IAST
{
public:
    /// Attribute name
    String name;
    /// Attribute type
    ASTPtr type;
    /// Attribute default value
    ASTPtr default_value;
    /// Attribute expression
    ASTPtr expression;
    /// Is attribute mirrored to the parent identifier
    bool hierarchical = false;
    /// Is hierarchical attribute bidirectional
    bool bidirectional = false;
    /// Flag that shows whether the id->attribute image is injective
    bool injective = false;
    /// MongoDB object ID
    bool is_object_id = false;

    String getID(char delim) const override { return "DictionaryAttributeDeclaration" + (delim + name); }

    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
