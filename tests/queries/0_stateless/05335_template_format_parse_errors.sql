-- Tests for ParsedTemplateFormatString error paths and verbosePrintString special characters.
--
-- The Template format supports inline format strings via format_template_row_format.
-- ParsedTemplateFormatString.cpp parses these strings and throws INVALID_TEMPLATE_FORMAT
-- on malformed input. The error message is built by dump(), which calls verbosePrintString()
-- to display delimiter content. The control characters used as delimiters exercise
-- verbosePrintString branches that were never reached in CI.
--
-- NOTE: these are client-side errors because SELECT output formatting is done by the
-- clickhouse client, not the server.
--
-- Covered code paths:
--   src/Formats/ParsedTemplateFormatString.cpp  lines 70-73   ($-sequence error)
--   src/Formats/ParsedTemplateFormatString.cpp  lines 89-92   (column terminator error)
--   src/Formats/ParsedTemplateFormatString.cpp  lines 119-120 (unbalanced brace)
--   src/Formats/ParsedTemplateFormatString.cpp  lines 157-182 (dump() loop body)
--   src/Formats/verbosePrintString.cpp          lines 26-28   (\b backspace)
--   src/Formats/verbosePrintString.cpp          lines 29-31   (\f form feed)
--   src/Formats/verbosePrintString.cpp          lines 35-37   (\r carriage return)
--   src/Formats/verbosePrintString.cpp          lines 53-54   (ASCII control < 0x20)
--   src/Formats/EscapingRuleUtils.cpp           line 43       (unknown escaping rule)
--   src/Formats/EscapingRuleUtils.cpp           lines 47-65   (escapingRuleToString all branches)

-- 1. '$' followed by a character that is neither '{' nor '$' triggers the
--    "Expected '{' or '$' after '$'" error (lines 70-73).
--    Delimiter '\r' triggers verbosePrintString carriage-return branch (lines 35-37).
--    One valid column is parsed before the error so dump() iterates the column list
--    (lines 157-182).
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='\r${a:CSV}$bad'; -- { clientError INVALID_TEMPLATE_FORMAT }

-- 2. A column name not followed by ':' (format rule) or '}' (close brace) triggers
--    the "Expected ':' or '}' after column name" error (lines 89-92).
--    Delimiter '\f' triggers verbosePrintString form-feed branch (lines 29-31).
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='\f${col#bad}'; -- { clientError INVALID_TEMPLATE_FORMAT }

-- 3. A format string that ends while parsing a column's escaping-rule section
--    (state=Format) triggers "Unbalanced parentheses" (lines 119-120).
--    Delimiter '\b' triggers verbosePrintString backspace branch (lines 26-28).
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='\b${a:CSV'; -- { clientError INVALID_TEMPLATE_FORMAT }

-- 4. Bell character (0x07) as delimiter is not in any named verbosePrintString case,
--    so it falls through to the generic ASCII-control branch (lines 53-54: if < 32).
--    The format string also ends in Column state ("${col" with no '}'), hitting
--    lines 119-120 again via a different state.
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='\a${col'; -- { clientError INVALID_TEMPLATE_FORMAT }

-- 5. An unrecognised escaping rule name triggers escapingRuleFromString() to throw
--    BAD_ARGUMENTS (EscapingRuleUtils.cpp line 43).
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='${a:UNKNOWN}'; -- { clientError BAD_ARGUMENTS }

-- 6. A format string with all seven valid escaping rules followed by a malformed
--    '$bad' triggers the dump() output loop, which calls escapingRuleToString() once
--    per column (EscapingRuleUtils.cpp lines 47-65, all seven case branches).
SELECT 1 AS a FORMAT Template SETTINGS format_template_row_format='${a:CSV}${a:JSON}${a:XML}${a:Escaped}${a:Quoted}${a:Raw}${a:None}$bad'; -- { clientError INVALID_TEMPLATE_FORMAT }
