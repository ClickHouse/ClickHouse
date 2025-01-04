SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_color = 1;
SET output_format_pretty_highlight_trailing_spaces = 1;
SET output_format_pretty_fallback_to_vertical = 0;

DROP TABLE IF EXISTS strings_whitespace;
CREATE TABLE strings_whitespace (str String) ENGINE = Memory;

INSERT INTO strings_whitespace VALUES ('string0'), ('string1 '), ('string2  '), ('string3     '), ('string4\n'), ('string5\r'),
('string6\t'), ('string7  \t'), ('string8\t\t'), ('string9   \n'), ('string10\t\n\r'),
('string11                                                                      '), ('string 12'), ('string   13   '),
(''), (' '), ('\n'), ('\r'), ('\t'), ('\t\t\t\n\r');

SELECT * FROM strings_whitespace FORMAT Pretty;
SELECT * FROM strings_whitespace FORMAT PrettyCompact;
SELECT * FROM strings_whitespace FORMAT PrettyMonoBlock;
SELECT * FROM strings_whitespace FORMAT PrettySpace;
SELECT * FROM strings_whitespace FORMAT PrettyCompactMonoBlock;
SELECT * FROM strings_whitespace FORMAT PrettySpaceMonoBlock;
SELECT * FROM strings_whitespace FORMAT Vertical;

DROP TABLE strings_whitespace;
