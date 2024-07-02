#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{

/// Parses information about target tables of a TimeSeries table.
/// Materialized views and window views have target tables too, however since
/// their create queries require special processing this parser is not used for them.
class ParserViewTargets : public IParserBase
{
public:
    using TargetKind = ViewTarget::Kind;

    explicit ParserViewTargets(TargetKind accept_kind1_, std::optional<TargetKind> accept_kind2_ = {}, std::optional<TargetKind> accept_kind3_ = {})
        : accept_kind1(accept_kind1_), accept_kind2(accept_kind2_), accept_kind3(accept_kind3_) {}

protected:
    const char * getName() const override { return "ViewTargets"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    TargetKind accept_kind1;
    std::optional<TargetKind> accept_kind2;
    std::optional<TargetKind> accept_kind3;
};

}
