#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{

/// Parses information about target tables (external or inner) of a materialized view or a window view.
/// The function parses one or multiple parts of a CREATE query looking like this:
///     TO db.table_name
///     TO INNER UUID 'XXX'
///     {ENGINE / INNER ENGINE} TableEngine(arguments) [ORDER BY ...] [SETTINGS ...]
/// Returns ASTViewTargets if succeeded.
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
