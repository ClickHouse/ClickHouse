select * from format(CustomSeparatedIgnoreSpaces, 'x String', ' unquoted_string\n') settings format_custom_escaping_rule='CSV';
