SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_color = 1;

DROP TABLE IF EXISTS strings_whitespace;
CREATE TABLE strings_whitespace (str String) ENGINE = Memory;

INSERT INTO strings_whitespace VALUES ('string0');
INSERT INTO strings_whitespace VALUES ('string1 ');
INSERT INTO strings_whitespace VALUES ('string2  ');
INSERT INTO strings_whitespace VALUES ('string3     ');
INSERT INTO strings_whitespace VALUES ('string4\n');
INSERT INTO strings_whitespace VALUES ('string5\r');
INSERT INTO strings_whitespace VALUES ('string6\t');
INSERT INTO strings_whitespace VALUES ('string7  \t');
INSERT INTO strings_whitespace VALUES ('string8\t\t');
INSERT INTO strings_whitespace VALUES ('string9   \n');
INSERT INTO strings_whitespace VALUES ('string10\t\n\r');
INSERT INTO strings_whitespace VALUES ('string11                                                                      ');
INSERT INTO strings_whitespace VALUES ('');
INSERT INTO strings_whitespace VALUES ('\n');
INSERT INTO strings_whitespace VALUES ('\r');
INSERT INTO strings_whitespace VALUES ('\t');
INSERT INTO strings_whitespace VALUES ('\t\t\t\n\r');

SELECT * FROM strings_whitespace FORMAT Pretty;
SELECT * FROM strings_whitespace FORMAT PrettyCompact;
SELECT * FROM strings_whitespace FORMAT PrettyMonoBlock;
SELECT * FROM strings_whitespace FORMAT PrettySpace;
SELECT * FROM strings_whitespace FORMAT PrettyCompactMonoBlock;
SELECT * FROM strings_whitespace FORMAT PrettySpaceMonoBlock;

DROP TABLE strings_whitespace;
