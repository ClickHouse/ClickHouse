#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** CREATE TYPE [IF NOT EXISTS | OR REPLACE] name[(parameters)] AS base_type */
class ASTCreateTypeQuery : public IAST
{
public:
    String name;
    /// The data type expression the type expands to; may reference `type_parameters` and other user-defined types.
    ASTPtr base_type;
    /// An expression list of identifiers, or nullptr for a type without parameters.
    ASTPtr type_parameters;
    bool if_not_exists{false};
    bool or_replace{false};

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
