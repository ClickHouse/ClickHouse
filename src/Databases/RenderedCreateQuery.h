#pragma once

#include <Common/SensitiveDataMasker.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <base/types.h>

#include <memory>

namespace DB
{

/// Everything the text of a CREATE query depends on besides the query itself.
struct RenderOptions
{
    /// Arguments of `IAST::formatWithPossiblyHidingSensitiveData`.
    bool one_line = true;
    bool show_secrets = false;
    bool print_pretty_type_names = false;
    IdentifierQuotingRule quoting_rule = IdentifierQuotingRule::WhenNecessary;
    IdentifierQuotingStyle quoting_style = IdentifierQuotingStyle::Backticks;

    /// Applied to the query before formatting it.
    bool show_uuid = false;

    /// Recorded only, so that a cached rendering is dropped when the masking rules change.
    std::shared_ptr<const SensitiveDataMasker> masker;

    bool operator==(const RenderOptions & other) const = default;
};

/// The columns of `system.tables` that are rendered from the CREATE query of a table.
struct RenderedCreateQuery
{
    String create_table_query;
    String engine_full;
    String as_select;
};

using RenderedCreateQueryPtr = std::shared_ptr<const RenderedCreateQuery>;

/// Which of the fields above a reader needs, so the other ones are left unformatted.
struct RenderedCreateQueryFields
{
    bool create_table_query = true;
    bool engine_full = true;
    bool as_select = true;

    static RenderedCreateQueryFields all() { return {}; }
};

/// The only place that reads the settings, so a rendering cannot disagree with its options.
RenderOptions resolveRenderOptions(const ContextPtr & context);

/// Modifies `ast` in place when the UUID has to be hidden. A null `ast` renders as empty strings.
RenderedCreateQueryPtr
renderCreateQuery(const ASTPtr & ast, const RenderOptions & options, RenderedCreateQueryFields fields = {});

}
