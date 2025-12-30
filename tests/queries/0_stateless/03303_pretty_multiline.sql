SET output_format_pretty_color = 1;
SET output_format_pretty_fallback_to_vertical = 0;
SELECT * FROM VALUES(('Hello', 'World'), ('Hel\nlo', 'World'), ('Hello\n', 'World'), ('\nHello\n\n', 'Wor  \n ld')) FORMAT Pretty;
SELECT * FROM VALUES(('Hello', 'World'), ('Hel\nlo', 'World'), ('Hello\n', 'World'), ('\nHello\n\n', 'Wor  \n ld')) FORMAT PrettyCompact;
SELECT * FROM VALUES(('Hello', 'World'), ('Hel\nlo', 'World'), ('Hello\n', 'World'), ('\nHello\n\n', 'Wor  \n ld')) FORMAT PrettySpace;
