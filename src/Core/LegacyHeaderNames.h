#pragma once

#include <Common/VersionNumber.h>
#include <Core/Names.h>


namespace DB
{

/** First version that quotes the parts of a plan column identifier when it needs quoting. A peer
  * older than this spells such an identifier without the quotes in a transport header.
  */
inline constexpr VersionNumber legacy_quoted_identifiers_version{26, 5, 0};

/** How a pre-26.5 server spells the plan column identifiers of one transport header.
  *
  * Servers before 26.5 concatenated a source alias and a column name without quoting them, so the
  * two versions disagree for exactly the names `backQuoteIfNeed` quotes. Built by
  * `buildLegacyHeaderNames` while the planner context that produced the header is alive.
  */
struct LegacyHeaderNames
{
    /// Legacy spelling of each header entry that the two versions spell differently.
    NameToNameMap current_to_legacy;

    /// Header entries for which several different legacy spellings were collected. Their owner
    /// cannot be determined, so a caller must refuse them rather than exchange them unchanged.
    NameSet ambiguous;

    bool empty() const { return current_to_legacy.empty() && ambiguous.empty(); }
};

}
