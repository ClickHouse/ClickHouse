#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/** A pair of the name and type. For example, browser FixedString(2).
  */
class ASTNameTypePair : public IAST
{
public:
    /// name
    String name;
    /// type
    ASTPtr type;
    /// Optional DEFAULT expression, e.g. `Tuple(s String DEFAULT 'Hello')`.
    /// This only exists at the syntax level: it is pulled up to the column level in
    /// InterpreterCreateQuery, and an attempt to build an actual data type while it is set throws.
    ASTPtr default_expression;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "NameTypePair" + (delim + name); }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// `type` and `default_expression` duplicate pointers held in `children`, so a visitor that
    /// replaces a child (e.g. the expansion of SQL UDFs) must update them as well.
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(nullptr, &type);
        f(nullptr, &default_expression);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

