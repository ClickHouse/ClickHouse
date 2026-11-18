#pragma once

#include <Common/VersionNumber.h>
#include <Core/Names.h>


namespace DB
{

/// First version that quotes the parts of a plan column identifier in a transport header.
inline constexpr VersionNumber legacy_quoted_identifiers_version{26, 5, 0};

/// How a pre-26.5 server spells the plan column identifiers of one transport header: it joined the
/// source alias and the column name without `backQuoteIfNeed`.
struct LegacyHeaderNames
{
    NameToNameMap current_to_legacy;

    /// Entries with no single legacy spelling. A caller must refuse them, never pass them unchanged.
    NameSet ambiguous;

    [[nodiscard]] bool empty() const { return current_to_legacy.empty() && ambiguous.empty(); }
};

}
