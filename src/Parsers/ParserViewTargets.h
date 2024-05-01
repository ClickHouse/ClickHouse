#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{

/// Parses information about target views of a table.
class ParserViewTargets : public IParserBase
{
public:
    ParserViewTargets();
    explicit ParserViewTargets(const std::vector<ViewTarget::Kind> & accept_kinds_) : accept_kinds(accept_kinds_) { }

protected:
    const char * getName() const override { return "ViewTargets"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    std::vector<ViewTarget::Kind> accept_kinds;
};

}
