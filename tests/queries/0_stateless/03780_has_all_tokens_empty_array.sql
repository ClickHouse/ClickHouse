SET enable_full_text_index = 1;

SELECT sum(hasAllTokens('', ''));
SELECT sum(hasAllTokens(toString(number), '')) FROM numbers(100);

SELECT sum(hasAllTokens('', []));
SELECT sum(hasAllTokens(toString(number), [])) FROM numbers(100);

SELECT sum(hasAnyTokens('', ''));
SELECT sum(hasAnyTokens(toString(number), '')) FROM numbers(100);

SELECT sum(hasAnyTokens('', []));
SELECT sum(hasAnyTokens(toString(number), [])) FROM numbers(100);
