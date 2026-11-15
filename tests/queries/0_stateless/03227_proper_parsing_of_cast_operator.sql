SELECT '414243'::String;
SELECT x'414243'::String;
SELECT b'01000001'::String;
SELECT '{"a": \'\x41\'}'::String;
SELECT '{"a": \'\x4\'}'::String; -- { clientError SYNTAX_ERROR }
SELECT '{"a": \'a\x4\'}'::String; -- { clientError SYNTAX_ERROR }
