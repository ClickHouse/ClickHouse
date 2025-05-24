#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

/** CREATE TYPE query */
class ASTCreateTypeQuery : public IAST
{
public:
    String name;
    ASTPtr base_type;
    ASTPtr type_parameters;
    ASTPtr input_expression;
    ASTPtr output_expression;
    ASTPtr default_expression;
    bool if_not_exists{false};
    bool or_replace{false};

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
