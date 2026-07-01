"""Helpers for making a perf-test ``CREATE TABLE`` backward-compatible.

The performance comparison runs the same test DDL against two servers: the
patched (PR) build and an older baseline build. When the PR introduces a new
``MergeTree`` setting, the baseline server does not know it and rejects the
``CREATE TABLE`` with ``UNKNOWN_SETTING``. To keep both sides comparable,
``perf.py`` strips such a setting from the query and retries, letting the old
server fall back to its own default.

The stripping is a correctness-critical baseline rewrite: a bug here would
silently rewrite the baseline ``CREATE TABLE`` while the PR side keeps the
original DDL, invalidating the comparison instead of failing fast. The scanner
therefore lives in this small importable module (rather than nested inside the
``perf.py`` script body, which executes top to bottom on import) so it can be
covered by unit tests in ``ci/tests/test_strip_setting_from_query.py``.
"""


def strip_setting_from_query(query, setting_name):
    """Strip a single MergeTree setting from a CREATE TABLE SETTINGS clause.

    Used to make a CREATE TABLE backward-compatible with an older server
    that does not know a newly-added MergeTree setting. The semantics on
    the old server fall back to its default for that setting, which is
    the desired behavior when the perf test wants to keep the old
    behavior on the PR side by setting the value explicitly.

    The value of a setting may legitimately contain commas inside quoted
    strings, parentheses, brackets, or braces (e.g. tuples, arrays,
    maps). Use a small scanner that tracks quote and bracket state so
    that the whole `name = value` assignment is removed even for such
    values. The same scanner first locates the `SETTINGS` keyword
    outside of any string/comment context, so an occurrence of the
    setting name inside, e.g., a column `COMMENT` literal preceding the
    clause cannot be matched.
    """

    def is_word_at(text, pos, word):
        """Case-insensitive word-boundary match of `word` at `pos`."""
        wlen = len(word)
        if text[pos : pos + wlen].upper() != word:
            return False
        if pos > 0 and (text[pos - 1].isalnum() or text[pos - 1] == "_"):
            return False
        end = pos + wlen
        return end >= len(text) or not (text[end].isalnum() or text[end] == "_")

    # Keywords that can follow the SETTINGS clause at the top level of a
    # CREATE TABLE (`AS SELECT ...`, `AS other_table`, `COMMENT '...'`,
    # `EMPTY AS SELECT ...`) and therefore terminate it.
    trailing_clause_keywords = ("AS", "COMMENT", "EMPTY")

    def find_table_settings_keyword(text):
        """Return the index of the table's own top-level `SETTINGS` keyword,
        ignoring matches inside string/backtick literals or `--`/`#`/`/* */`
        comments and inside brackets. Return -1 when there is no table-level
        SETTINGS clause -- including when a top-level `AS` / `COMMENT` /
        `EMPTY` clause is reached first, in which case any later `SETTINGS` is
        a query-level clause (e.g. on `AS SELECT ...`), not the table's own.
        Treating that as "not found" makes the caller leave the query
        unchanged so `perf.py` fails fast on a misplaced setting instead of
        silently stripping a query-level clause. Bracket depth is tracked so a
        column-level `COMMENT` inside the schema parens does not end the scan
        early."""
        i = 0
        n = len(text)
        quote = None
        line_comment = False
        block_comment = False
        depth = 0
        while i < n:
            c = text[i]
            next_c = text[i + 1] if i + 1 < n else ""
            if line_comment:
                if c == "\n":
                    line_comment = False
                i += 1
                continue
            if block_comment:
                if c == "*" and next_c == "/":
                    block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if quote is not None:
                if c == "\\" and i + 1 < n:
                    i += 2
                    continue
                if c == quote:
                    # `''` inside a single-quoted string is an escaped quote.
                    if quote == "'" and next_c == "'":
                        i += 2
                        continue
                    quote = None
                i += 1
                continue
            if (c == "-" and next_c == "-") or c == "#":
                line_comment = True
                i += 1
                continue
            if c == "/" and next_c == "*":
                block_comment = True
                i += 2
                continue
            if c in "'\"`":
                quote = c
                i += 1
                continue
            if c in "([{":
                depth += 1
                i += 1
                continue
            if c in ")]}":
                if depth > 0:
                    depth -= 1
                i += 1
                continue
            if depth == 0:
                if is_word_at(text, i, "SETTINGS"):
                    return i
                # A top-level trailing clause reached before any table-level
                # SETTINGS: the table has none of its own, so a following
                # SETTINGS belongs to `AS SELECT ...` and must not be edited.
                if any(is_word_at(text, i, kw) for kw in trailing_clause_keywords):
                    return -1
            i += 1
        return -1

    settings_pos = find_table_settings_keyword(query)
    if settings_pos < 0:
        return query

    # Locate `setting_name = ` at the top level of the SETTINGS clause:
    # outside string and backtick literals, outside `--`, `#`, `/* */`
    # comments, and at bracket depth 0 (so a literal containing the
    # setting name inside an array, tuple, or function call is not
    # matched). The plain `re.search` cannot enforce this on raw text.
    name_lower = setting_name.lower()
    name_len = len(setting_name)
    i = settings_pos + len("SETTINGS")
    n = len(query)
    quote = None
    line_comment = False
    block_comment = False
    depth = 0
    name_start = -1
    value_start = -1
    last_top_level_comma = -1
    while i < n:
        c = query[i]
        next_c = query[i + 1] if i + 1 < n else ""
        if line_comment:
            if c == "\n":
                line_comment = False
            i += 1
            continue
        if block_comment:
            if c == "*" and next_c == "/":
                block_comment = False
                i += 2
                continue
            i += 1
            continue
        if quote is not None:
            if c == "\\" and i + 1 < n:
                i += 2
                continue
            if c == quote:
                if quote == "'" and next_c == "'":
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if (c == "-" and next_c == "-") or c == "#":
            line_comment = True
            i += 1
            continue
        if c == "/" and next_c == "*":
            block_comment = True
            i += 2
            continue
        if c in "'\"`":
            quote = c
            i += 1
            continue
        if c in "([{":
            depth += 1
            i += 1
            continue
        if c in ")]}":
            if depth == 0:
                break
            depth -= 1
            i += 1
            continue
        if (
            depth == 0
            and i > settings_pos + len("SETTINGS")
            and any(is_word_at(query, i, kw) for kw in trailing_clause_keywords)
        ):
            # A trailing clause (`AS SELECT ...`, `COMMENT '...'`) ends the
            # SETTINGS clause. Stop the name scan here, exactly as the value
            # scan below does, so the setting name is only matched inside the
            # table's own SETTINGS. Without this, the scan would run past
            # `AS SELECT ...` and could match the name inside a query-level
            # SETTINGS or, worse, cut from a comma in the SELECT column list,
            # silently rewriting the baseline DDL instead of failing fast.
            break
        if depth == 0 and c == ",":
            # Top-level comma: the separator between two settings. Remember
            # the last one before the matched name so the cut can remove the
            # whole separator (including any comment between it and the name).
            last_top_level_comma = i
            i += 1
            continue
        if depth == 0 and query[i : i + name_len].lower() == name_lower:
            before_ok = i == settings_pos + len("SETTINGS") or not (
                query[i - 1].isalnum() or query[i - 1] == "_"
            )
            after_idx = i + name_len
            if (
                before_ok
                and after_idx < n
                and not (query[after_idx].isalnum() or query[after_idx] == "_")
            ):
                j = after_idx
                while j < n and query[j] in " \t\r\n":
                    j += 1
                if j < n and query[j] == "=":
                    j += 1
                    while j < n and query[j] in " \t\r\n":
                        j += 1
                    name_start = i
                    value_start = j
                    break
        i += 1

    if name_start < 0:
        return query

    # When the dropped assignment is not the first entry in the SETTINGS
    # clause, cut from the top-level comma that separates it from the
    # previous entry, so the whole separator (the comma plus any whitespace
    # or comments between it and the setting name) is removed cleanly. The
    # comma position comes from the comment- and literal-aware forward scan
    # above, so a `/* ... */` comment sitting in the separator does not
    # defeat it. A plain backward whitespace scan, by contrast, would stop
    # at such a comment and leave the orphaned comma (and comment) behind.
    if last_top_level_comma >= 0:
        cut_start = last_top_level_comma
    else:
        cut_start = name_start

    # Scan from the start of the value to find its end at the next
    # top-level comma or semicolon, a keyword starting a trailing
    # clause, or end of query. Mirror the name-scan state machine so
    # commas inside string/backtick literals or inside `--`, `#`,
    # `/* */` comments are ignored.
    i = value_start
    quote = None
    line_comment = False
    block_comment = False
    depth = 0
    while i < n:
        c = query[i]
        next_c = query[i + 1] if i + 1 < n else ""
        if line_comment:
            if c == "\n":
                line_comment = False
            i += 1
            continue
        if block_comment:
            if c == "*" and next_c == "/":
                block_comment = False
                i += 2
                continue
            i += 1
            continue
        if quote is not None:
            if c == "\\" and i + 1 < n:
                i += 2
                continue
            if c == quote:
                # `''` inside a single-quoted string is an escaped quote.
                if quote == "'" and next_c == "'":
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if (c == "-" and next_c == "-") or c == "#":
            line_comment = True
            i += 1
            continue
        if c == "/" and next_c == "*":
            block_comment = True
            i += 2
            continue
        if c in "'\"`":
            quote = c
            i += 1
            continue
        if c in "([{":
            depth += 1
            i += 1
            continue
        if c in ")]}":
            if depth == 0:
                break
            depth -= 1
            i += 1
            continue
        if depth == 0 and c in ",;":
            break
        if (
            depth == 0
            and i > value_start
            and any(is_word_at(query, i, kw) for kw in trailing_clause_keywords)
        ):
            # Leave the whitespace before the keyword in place so the
            # preceding token is not glued to the trailing clause.
            while i > value_start and query[i - 1] in " \t\r\n":
                i -= 1
            break
        i += 1

    # When the dropped assignment is the first entry of the SETTINGS clause
    # (no preceding top-level comma to cut from, so `cut_start` stayed at
    # `name_start`), consume the trailing comma and any whitespace after it
    # so the next entry is not left with a stray leading separator.
    if cut_start == name_start and i < n and query[i] == ",":
        i += 1
        while i < n and query[i] in " \t\r\n":
            i += 1

    stripped = query[:cut_start] + query[i:]

    # The cleanup below is anchored at the known position of the
    # `SETTINGS` keyword (which is unchanged by the cut, as the cut
    # starts after it) instead of a global regex over the whole query:
    # the query may legitimately contain `SETTINGS` or commas inside
    # string literals (e.g. in a column `COMMENT`), which a global
    # regex would corrupt.
    def skip_whitespace_and_comments(text, pos):
        length = len(text)
        while pos < length:
            c = text[pos]
            next_c = text[pos + 1] if pos + 1 < length else ""
            if c in " \t\r\n":
                pos += 1
            elif (c == "-" and next_c == "-") or c == "#":
                while pos < length and text[pos] != "\n":
                    pos += 1
            elif c == "/" and next_c == "*":
                pos += 2
                while pos < length:
                    if text[pos] == "*" and pos + 1 < length and text[pos + 1] == "/":
                        pos += 2
                        break
                    pos += 1
            else:
                break
        return pos

    after_keyword = settings_pos + len("SETTINGS")

    # Drop a stray leading `,` that the structural cut above may have
    # missed in unusual whitespace/comment shapes.
    rest = skip_whitespace_and_comments(stripped, after_keyword)
    if rest < len(stripped) and stripped[rest] == ",":
        stripped = stripped[:rest] + stripped[rest + 1 :]

    # Drop an empty SETTINGS clause (the only setting was the dropped one),
    # along with any whitespace that preceded the SETTINGS keyword. The
    # clause is considered empty when only whitespace or comments remain
    # between `SETTINGS` and the terminating `;`/end/trailing clause, so
    # a comment that sat between `SETTINGS` and the removed first setting
    # is dropped too instead of being left as a stray `SETTINGS /*...*/ ;`.
    rest = skip_whitespace_and_comments(stripped, after_keyword)
    if rest >= len(stripped) or stripped[rest] == ";":
        lead = settings_pos
        while lead > 0 and stripped[lead - 1] in " \t\r\n":
            lead -= 1
        stripped = stripped[:lead] + stripped[rest:]
    elif any(is_word_at(stripped, rest, kw) for kw in trailing_clause_keywords):
        # A trailing clause (`AS SELECT ...`, `COMMENT '...'`) follows the
        # now-empty SETTINGS clause; keep the whitespace before `SETTINGS`
        # as the separator.
        stripped = stripped[:settings_pos] + stripped[rest:]

    return stripped
