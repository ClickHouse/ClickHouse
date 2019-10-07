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
    /// Is atribute mirrored to the parent identifier
    bool hierarchical;
    /// Flag that shows whether the id->attribute image is injective
    bool injective;
    /// MongoDB object ID
    bool is_object_id;

    String getID(char delim) const override { return "DictionaryAttributeDeclaration" + (delim + name); }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


//class ASTDictionaryAttributesDeclarations : public IAST
//{
//public:
//    ASTExpressionList * attributes = nullptr;
//
//    String getID(char) const override { return "Dictionary attributes definition"; }
//
//    ASTPtr clone() const override;
//
//    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
//};

}
