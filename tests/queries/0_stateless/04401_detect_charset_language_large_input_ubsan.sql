-- Tags: no-fasttest
-- Tag no-fasttest: depends on nlp-data

-- Inputs larger than the internal max_string_size (32768 bytes) whose bigrams match no
-- encoding well leave the result empty. The empty result is expected; this just checks that
-- detectCharset / detectLanguageUnknown do not pass a null pointer to memcpy (UBSan nonnull).

SET allow_experimental_nlp_functions = 1;

SELECT detectCharset(materialize(repeat('Ω', 20000)));
SELECT detectLanguageUnknown(materialize(repeat('Ω', 20000)));
SELECT detectCharset(materialize(repeat('蝴', 14000)));
SELECT detectLanguageUnknown(materialize(repeat('蝴', 14000)));

-- Normal inputs still detect correctly.
SELECT detectCharset('Ich bleibe für ein paar Tage.');
SELECT detectLanguageUnknown('Ich bleibe für ein paar Tage.');
