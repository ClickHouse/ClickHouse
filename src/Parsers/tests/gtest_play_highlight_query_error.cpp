#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>

/** Regression coverage for the syntax-error highlighting logic in `programs/server/play.html`.
  *
  * When a query fails, the server returns a message like `failed at position N (...)` (see
  * `writeCommonErrorMessage` in `src/Parsers/parseQuery.cpp`). `play.html` parses that hint and
  * paints the offending position in the query editor. Two pure functions do the interesting work:
  *
  *   - `byteOffsetToCharIndex`: the server reports a 1-based UTF-8 *byte* offset, but the editor
  *     indexes text in UTF-16 code units, so the offset must be converted before it can be used.
  *   - `highlightQueryError`: resolves where the sent query sits inside the full textarea contents
  *     (preferring the caller-captured start offset so duplicate statements disambiguate), maps the
  *     byte offset with the function above, and detects the `(end of query)` boundary case (the
  *     parser reached the end of the statement or its `;` separator) so the marker lands on the
  *     statement boundary rather than filling the `;` / whitespace token that starts there.
  *     It also arbitrates the single editor marker between several concurrently failing statements
  *     of one run (`postMulti` executes parallelizable groups with `Promise.all`, so error
  *     responses arrive in an arbitrary order): the leftmost failing statement wins
  *     deterministically, and `beginFlight` resets the arbitration at run start.
  *
  * There is no JavaScript runtime in CI, so — exactly as `gtest_play_get_query_under_cursor.cpp`
  * does for `getQueryUnderCursor` — we reproduce these pure algorithms here and lock their
  * contracts. Keep this in sync with the corresponding functions in `programs/server/play.html`.
  *
  * The stateful clear/repaint interaction (`editorInteractionGen`, `suppressReturnToEditor`, the
  * completion capture handler) is DOM-bound and cannot run here; only its numeric staleness guard
  * — a late response paints only if the user has not returned to the editor since the run began —
  * is portable and covered by `StaleResponseGuard` below.
  */

namespace
{

/// Faithful port of `byteOffsetToCharIndex` from play.html. Walks `str` (a UTF-16 string, as in
/// the browser) one Unicode code point at a time, accumulating each code point's UTF-8 byte
/// length, and returns the UTF-16 code-unit index just past the code point that reaches
/// `byte_offset`. Astral code points occupy two UTF-16 units, matching the JS original.
size_t byteOffsetToCharIndex(const std::u16string & str, size_t byte_offset)
{
    if (byte_offset == 0)
        return 0;

    size_t bytes = 0;
    size_t i = 0; /// UTF-16 code-unit index (== the JS string index).
    while (i < str.size())
    {
        const char16_t unit = str[i];
        uint32_t cp = 0;
        size_t units = 0;
        /// Combine a high/low surrogate pair into a single astral code point.
        if (unit >= 0xD800 && unit <= 0xDBFF && i + 1 < str.size()
            && str[i + 1] >= 0xDC00 && str[i + 1] <= 0xDFFF)
        {
            cp = 0x10000 + ((static_cast<uint32_t>(unit - 0xD800) << 10) | static_cast<uint32_t>(str[i + 1] - 0xDC00));
            units = 2;
        }
        else
        {
            cp = unit;
            units = 1;
        }

        size_t b = 0;
        if (cp <= 0x7F)
            b = 1;
        else if (cp <= 0x7FF)
            b = 2;
        else if (cp <= 0xFFFF)
            b = 3;
        else
            b = 4;

        bytes += b;
        i += units;
        if (bytes >= byte_offset)
            return i;
    }
    return str.size();
}

struct QueryErrorPos
{
    bool found = false;  /// Did the error text carry a `failed at position N` hint we could map?
    size_t pos = 0;      /// Absolute UTF-16 index into the full textarea text.
    bool at_end = false; /// Was it an `(end of query)` boundary error (mark the boundary, not a token)?
    size_t base = 0;     /// Resolved start offset of the sent query within the text (valid when `found`).
};

/// Faithful port of the pure computation in `highlightQueryError` (play.html).
QueryErrorPos computeQueryErrorPos(
    const std::string & error_text, const std::u16string & query, std::optional<size_t> query_start, const std::u16string & text)
{
    QueryErrorPos result;

    const std::string needle = "failed at position ";
    const size_t p = error_text.find(needle);
    if (p == std::string::npos)
        return result;

    size_t d = p + needle.size();
    if (d >= error_text.size() || !std::isdigit(static_cast<unsigned char>(error_text[d])))
        return result;

    size_t value = 0;
    while (d < error_text.size() && std::isdigit(static_cast<unsigned char>(error_text[d])))
    {
        value = value * 10 + static_cast<size_t>(error_text[d] - '0');
        ++d;
    }

    /// 1-based byte offset -> 0-based; a reported position of 0 is not a valid offset.
    if (value == 0)
        return result;
    const size_t byte_offset = value - 1;

    /// The server writes " (end of query)" for both an unexpected end of stream and a `;`.
    result.at_end = error_text.find(" (end of query)") != std::string::npos;

    /// Locate the sent query within the textarea. Prefer the exact captured start offset so
    /// duplicate statements map to the one that failed; otherwise fall back to its first
    /// occurrence, and if it is gone entirely do not paint a stale position.
    size_t base = 0;
    if (query_start && *query_start <= text.size() && text.compare(*query_start, query.size(), query) == 0)
    {
        base = *query_start;
    }
    else
    {
        const size_t found_at = text.find(query);
        if (found_at == std::u16string::npos)
            return result;
        base = found_at;
    }

    const size_t char_index = byteOffsetToCharIndex(query, byte_offset);
    result.found = true;
    result.base = base;
    result.pos = std::min(base + char_index, text.size());
    return result;
}

/// The marker state one run accumulates across error responses. `owner_start` mirrors
/// `queryErrorOwnerStart` in play.html: empty (-1 in the JS) until a statement claims the marker.
/// `beginFlight` resets it at run start; here a fresh instance models a fresh run.
struct QueryErrorMarker
{
    std::optional<size_t> owner_start;
    QueryErrorPos painted; /// `found == false` while nothing has been painted.
};

/// Faithful port of the run-level arbitration in `highlightQueryError` (play.html): the editor has
/// a single marker, and several statements of one run can fail concurrently (`postMulti` executes
/// parallelizable groups with `Promise.all`), so error responses arrive in an arbitrary order. A
/// response may take the marker only if no statement has claimed it yet in this run, or if its
/// statement starts earlier in the editor than the current owner's — the leftmost failing
/// statement wins regardless of arrival order.
void applyQueryError(
    QueryErrorMarker & marker,
    const std::string & error_text,
    const std::u16string & query,
    std::optional<size_t> query_start,
    const std::u16string & text)
{
    const auto r = computeQueryErrorPos(error_text, query, query_start, text);
    if (!r.found)
        return;
    if (marker.owner_start && r.base >= *marker.owner_start)
        return;
    marker.owner_start = r.base;
    marker.painted = r;
}

/// Port of the staleness guard in `postImpl`: a late error response paints its highlight only
/// while `editorInteractionGen === runEditorInteraction`, i.e. the interaction generation
/// snapshotted at flight start still matches the current one (the user has not returned to the
/// editor since the run began).
bool wouldPaintErrorHighlight(int generation_at_flight_start, int generation_now)
{
    return generation_at_flight_start == generation_now;
}

/// UTF-8 byte length of a UTF-16 string, to spell out expected byte offsets in the tests below.
size_t utf8Bytes(const std::u16string & str)
{
    size_t bytes = 0;
    for (size_t i = 0; i < str.size();)
    {
        const char16_t unit = str[i];
        uint32_t cp = 0;
        size_t units = 0;
        if (unit >= 0xD800 && unit <= 0xDBFF && i + 1 < str.size() && str[i + 1] >= 0xDC00 && str[i + 1] <= 0xDFFF)
        {
            cp = 0x10000 + ((static_cast<uint32_t>(unit - 0xD800) << 10) | static_cast<uint32_t>(str[i + 1] - 0xDC00));
            units = 2;
        }
        else
        {
            cp = unit;
            units = 1;
        }
        bytes += cp <= 0x7F ? 1 : cp <= 0x7FF ? 2 : cp <= 0xFFFF ? 3 : 4;
        i += units;
    }
    return bytes;
}

}

TEST(PlayByteOffsetToCharIndex, Ascii)
{
    EXPECT_EQ(byteOffsetToCharIndex(u"SELECT", 0), 0u);
    EXPECT_EQ(byteOffsetToCharIndex(u"SELECT", 1), 1u);
    EXPECT_EQ(byteOffsetToCharIndex(u"SELECT", 6), 6u);
    /// Past the end clamps to the string length.
    EXPECT_EQ(byteOffsetToCharIndex(u"SELECT", 100), 6u);
}

TEST(PlayByteOffsetToCharIndex, TwoAndThreeByteCodePoints)
{
    /// Cyrillic small letter yu (U+044E) is two UTF-8 bytes but one UTF-16 unit.
    EXPECT_EQ(byteOffsetToCharIndex(u"ю", 2), 1u);
    /// `a<ю>b`: the byte offset of `b` (3) maps back to UTF-16 index 2. The Cyrillic letter is
    /// written with the `\u044E` escape (the same U+044E as on the line above) so it is not adjacent
    /// to a Latin letter in the source — the style check forbids Cyrillic characters hiding in Latin.
    EXPECT_EQ(byteOffsetToCharIndex(u"a\u044Eb", 3), 2u);
    /// CJK ideograph (U+4E2D) is three UTF-8 bytes, one UTF-16 unit.
    EXPECT_EQ(byteOffsetToCharIndex(u"中", 3), 1u);
    EXPECT_EQ(byteOffsetToCharIndex(u"a中b", 4), 2u);
}

TEST(PlayByteOffsetToCharIndex, AstralCodePointsAreTwoUtf16Units)
{
    /// Grinning face (U+1F600) is four UTF-8 bytes and a surrogate pair (two UTF-16 units).
    EXPECT_EQ(byteOffsetToCharIndex(u"😀", 4), 2u);
    /// `a<😀>b`: the byte offset of `b` (5) maps back to UTF-16 index 3 (1 + 2 surrogate units).
    EXPECT_EQ(byteOffsetToCharIndex(u"a😀b", 5), 3u);
}

TEST(PlayHighlightQueryError, NoPositionHint)
{
    /// A message without a `failed at position N` hint yields nothing to paint.
    EXPECT_FALSE(computeQueryErrorPos("Some unrelated error", u"SELECT 1", 0, u"SELECT 1").found);
}

TEST(PlayHighlightQueryError, PlainTokenError)
{
    /// `SELECT foo bar` fails at `bar` (1-based position 12). Not an end-of-query error, so the
    /// token at that offset is painted (`at_end` stays false).
    const std::u16string text = u"SELECT foo bar";
    const auto r = computeQueryErrorPos("Syntax error: failed at position 12 (bar)", text, 0, text);
    EXPECT_TRUE(r.found);
    EXPECT_FALSE(r.at_end);
    EXPECT_EQ(r.pos, 11u); /// 0-based index of `bar`.
}

TEST(PlayHighlightQueryError, EndOfQuerySemicolonMarksBoundaryNotSeparator)
{
    /// pamarcos' example: `SELECT; SELECT 1`, running the first statement sends `SELECT;` (the
    /// run-under-cursor path keeps the trailing `;`). The server reports position 7 (end of
    /// query); the marker must land on the statement boundary (index 6, before the `;`), and
    /// `at_end` must be set so the UI draws a boundary marker instead of filling the `;` token.
    const std::u16string text = u"SELECT; SELECT 1";
    const auto r = computeQueryErrorPos("Syntax error: failed at position 7 (end of query)", u"SELECT;", 0, text);
    EXPECT_TRUE(r.found);
    EXPECT_TRUE(r.at_end);
    EXPECT_EQ(r.pos, 6u);
}

TEST(PlayHighlightQueryError, EndOfQueryEndOfStream)
{
    /// The "run all" / selection path sends the trimmed statement without the `;` (`SELECT`).
    /// The server still reports position 7 (end of query); the boundary is the end of the sent
    /// text, and both send paths converge on the same absolute position (6) and `at_end` flag.
    const std::u16string text = u"SELECT; SELECT 1";
    const auto r = computeQueryErrorPos("Syntax error: failed at position 7 (end of query)", u"SELECT", 0, text);
    EXPECT_TRUE(r.found);
    EXPECT_TRUE(r.at_end);
    EXPECT_EQ(r.pos, 6u);
}

TEST(PlayHighlightQueryError, EndOfQueryAtEndOfText)
{
    /// A single statement with no separator: the boundary is at the very end of the text.
    const std::u16string text = u"SELECT";
    const auto r = computeQueryErrorPos("Syntax error: failed at position 7 (end of query)", text, 0, text);
    EXPECT_TRUE(r.found);
    EXPECT_TRUE(r.at_end);
    EXPECT_EQ(r.pos, text.size()); /// 6 == end of text; the marker is emitted after the last token.
}

TEST(PlayHighlightQueryError, DuplicateStatementsDisambiguateByStartOffset)
{
    /// Two identical statements. The captured start offset selects the *second* occurrence; without
    /// it, the first match wins. Locks the fix for `SELECT 1; SELECT 1`-style duplicates.
    const std::u16string text = u"SELCT; SELCT"; /// `SELCT` at UTF-16 offsets 0 and 7.
    const auto second = computeQueryErrorPos("Syntax error: failed at position 1 (SELCT)", u"SELCT", 7, text);
    EXPECT_TRUE(second.found);
    EXPECT_EQ(second.pos, 7u);

    const auto first = computeQueryErrorPos("Syntax error: failed at position 1 (SELCT)", u"SELCT", std::nullopt, text);
    EXPECT_TRUE(first.found);
    EXPECT_EQ(first.pos, 0u);
}

TEST(PlayHighlightQueryError, StartOffsetMismatchFallsBackToFirstOccurrence)
{
    /// If the editor changed while the request was in flight so the query is no longer at the
    /// captured offset, fall back to its first occurrence.
    const std::u16string text = u"  SELECT 1";
    const auto r = computeQueryErrorPos("Syntax error: failed at position 1 (SELECT)", u"SELECT 1", 999, text);
    EXPECT_TRUE(r.found);
    EXPECT_EQ(r.pos, 2u);
}

TEST(PlayHighlightQueryError, QueryGoneFromEditorPaintsNothing)
{
    /// The sent query is no longer present in the textarea: do not paint a stale position.
    EXPECT_FALSE(computeQueryErrorPos("Syntax error: failed at position 1 (SELECT)", u"SELECT 1", 0, u"SELECT 2").found);
}

TEST(PlayHighlightQueryError, UnicodeOffsetIsConverted)
{
    /// `ю=1` (the `ю` is two UTF-8 bytes). The server reports byte position 3 for the `=`; the
    /// marker must map back to UTF-16 index 1, not 2.
    const std::u16string text = u"ю=1";
    ASSERT_EQ(utf8Bytes(u"ю"), 2u);
    const auto r = computeQueryErrorPos("Syntax error: failed at position 3 (=)", text, 0, text);
    EXPECT_TRUE(r.found);
    EXPECT_FALSE(r.at_end);
    EXPECT_EQ(r.pos, 1u);
}

TEST(PlayHighlightQueryError, StaleResponseGuard)
{
    /// No return to the editor since the run began -> the late response may paint.
    EXPECT_TRUE(wouldPaintErrorHighlight(/* snapshot */ 3, /* current */ 3));
    /// A key consumed by the completion handler (or any return to the editor) bumps the
    /// generation past the snapshot -> the late response is suppressed.
    EXPECT_FALSE(wouldPaintErrorHighlight(/* snapshot */ 3, /* current */ 4));
}

TEST(PlayHighlightQueryError, ConcurrentFailuresPaintDeterministically)
{
    /// Two parallelizable statements (`postMulti` runs consecutive SELECTs concurrently) both fail
    /// with position-bearing syntax errors, and the editor has a single marker. The final marker
    /// must be the same whichever HTTP response arrives first: the leftmost failing statement owns
    /// it, so the highlight is deterministic instead of timing-dependent.
    const std::u16string text = u"SELECT * FRM t; SELECT * FRM u";
    const std::string err1 = "Syntax error: failed at position 10 ('FRM')";
    const std::string err2 = "Syntax error: failed at position 10 ('FRM')";

    QueryErrorMarker first_response_first;
    applyQueryError(first_response_first, err1, u"SELECT * FRM t", 0, text);
    applyQueryError(first_response_first, err2, u"SELECT * FRM u", 16, text);

    QueryErrorMarker second_response_first;
    applyQueryError(second_response_first, err2, u"SELECT * FRM u", 16, text);
    applyQueryError(second_response_first, err1, u"SELECT * FRM t", 0, text);

    ASSERT_TRUE(first_response_first.painted.found);
    ASSERT_TRUE(second_response_first.painted.found);
    /// Both arrival orders converge on the first statement's `FRM` (absolute index 9); the second
    /// statement's error (absolute index 25) never wins.
    EXPECT_EQ(first_response_first.painted.pos, 9u);
    EXPECT_EQ(second_response_first.painted.pos, 9u);
    EXPECT_EQ(first_response_first.owner_start, std::optional<size_t>(0));
    EXPECT_EQ(second_response_first.owner_start, std::optional<size_t>(0));
}

TEST(PlayHighlightQueryError, LaterStatementDoesNotStealTheMarker)
{
    /// Once the leftmost failing statement has painted, a later statement's error response —
    /// however late it arrives — must not move the marker, nor flip its `at_end` flavor.
    const std::u16string text = u"SELECT * FRM t; SELECT;";
    QueryErrorMarker marker;
    applyQueryError(marker, "Syntax error: failed at position 10 ('FRM')", u"SELECT * FRM t", 0, text);
    applyQueryError(marker, "Syntax error: failed at position 7 (end of query)", u"SELECT;", 16, text);
    ASSERT_TRUE(marker.painted.found);
    EXPECT_EQ(marker.painted.pos, 9u);
    EXPECT_FALSE(marker.painted.at_end);
}

TEST(PlayHighlightQueryError, NonPositionErrorDoesNotBlockAPositionBearingOne)
{
    /// The arbitration is among position-bearing errors only: a message without a `failed at
    /// position N` hint claims nothing (there is nothing to paint for it), so a later statement's
    /// mappable error still paints.
    const std::u16string text = u"SELECT throwIf(1); SELECT * FRM u";
    QueryErrorMarker marker;
    applyQueryError(marker, "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero", u"SELECT throwIf(1)", 0, text);
    applyQueryError(marker, "Syntax error: failed at position 10 ('FRM')", u"SELECT * FRM u", 19, text);
    ASSERT_TRUE(marker.painted.found);
    EXPECT_EQ(marker.painted.pos, 28u); /// `FRM` of the second statement.
}

TEST(PlayHighlightQueryError, ArbitrationResetsAtRunStart)
{
    /// `beginFlight` resets `queryErrorOwnerStart` at run start, so an earlier run's owner cannot
    /// suppress the next run's highlight. A fresh `QueryErrorMarker` models the reset.
    const std::u16string text = u"SELECT 1; SELECT * FRM u";
    QueryErrorMarker previous_run;
    applyQueryError(previous_run, "Syntax error: failed at position 1 ('SELECT')", u"SELECT 1", 0, text);
    EXPECT_EQ(previous_run.owner_start, std::optional<size_t>(0));

    QueryErrorMarker next_run;
    applyQueryError(next_run, "Syntax error: failed at position 10 ('FRM')", u"SELECT * FRM u", 10, text);
    ASSERT_TRUE(next_run.painted.found);
    EXPECT_EQ(next_run.painted.pos, 19u);
}
