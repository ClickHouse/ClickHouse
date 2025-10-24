#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** An argument of Object data type declaration (for example for JSON). Can contain one of:
 *  - pair (path, data type)
 *  - path that should be skipped
 *  - path regexp for paths that should be skipped
 *  - setting in a form of `setting=N`
 */
class ASTObjectTypeArgument : public IAST
{
public:
    ASTPtr path_with_type;
    ASTPtr skip_path;
    ASTPtr skip_path_regexp;
    ASTPtr parameter;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ASTObjectTypeArgument"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

