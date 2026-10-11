-- Tags: no-fasttest

-- Experimental gate
SELECT htmlParse('<html></html>'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_html_functions = 1;

-- Empty input
SELECT length(htmlParse(''));

-- Empty document gets <head></head><body></body> filled in
SELECT htmlParse('<html></html>');

-- Well-formed document round-trips
SELECT htmlParse('<html><head><title>t</title></head><body><p>hi</p></body></html>');

-- Malformed: missing close tags are added
SELECT htmlParse('<html><body><p>unclosed');

-- Fragment input: lexbor auto-wraps into a full document
SELECT htmlParse('<p>bare</p>');

-- Parser is idempotent after first pass
SELECT htmlParse(htmlParse('<html><head><title>t</title></head><body><p>hi</p></body></html>'))
     = htmlParse('<html><head><title>t</title></head><body><p>hi</p></body></html>');

-- Illegal type
SELECT htmlParse(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Batch: all 100 rows succeed with the reused parser document
SELECT count() FROM (SELECT htmlParse('<div>' || toString(number) || '</div>') FROM numbers(100));
