-- Tags: no-fasttest
-- Tag no-fasttest: depends on cld2

-- https://github.com/ClickHouse/ClickHouse/issues/103765

SET allow_experimental_nlp_functions = 1;

SELECT detectLanguageMixed(materialize('abcdα'));
SELECT detectLanguageMixed(materialize('abcdℵ'));
SELECT detectLanguageMixed(materialize('helloαβγ'));
SELECT detectLanguageMixed(materialize(CAST('Ðь' AS String)));
SELECT detectLanguageMixed(materialize(CAST('ނJ' AS String)));
SELECT detectLanguageMixed(materialize(CAST(unhex('C390D18C') AS String)));
SELECT detectLanguageMixed(materialize(CAST(unhex('DE824A') AS String)));
SELECT detectLanguageMixed(materialize(CAST(unhex('05DE824A') AS String)));
