CSV
---

Comma separated values (`RFC <https://tools.ietf.org/html/rfc4180>`_).

String values are output in double quotes. Double quote inside a string is output as two consecutive double quotes. That's all escaping rules. Date and DateTime values are output in double quotes. Numbers are output without quotes. Fields are delimited by commas. Rows are delimited by unix newlines (LF). Arrays are output in following way: first, array are serialized to String (as in TabSeparated or Values formats), and then the String value are output in double quotes. Tuples are narrowed and serialized as separate columns.

During parsing, values could be enclosed or not enclosed in quotes. Supported both single and double quotes. In particular, Strings could be represented without quotes - in that case, they are parsed up to comma or newline (CR or LF). Contrary to RFC, in case of parsing strings without quotes, leading and trailing spaces and tabs are ignored. As line delimiter, both Unix (LF), Windows (CR LF) or Mac OS Classic (LF CR) variants are supported.

CSV format supports output of totals and extremes similar to TabSeparated format.
