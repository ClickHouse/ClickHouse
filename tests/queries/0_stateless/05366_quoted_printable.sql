-- Basic encoding: '=', non-ASCII bytes and control characters are escaped, other printable ASCII is kept.
SELECT encodeQuotedPrintable('Café = 3 €');
SELECT encodeQuotedPrintable('');
SELECT encodeQuotedPrintable(unhex('007F80FF'));

-- A space or tab is escaped only where it would end a line.
SELECT encodeQuotedPrintable('trailing space ');
SELECT encodeQuotedPrintable('tab\tin the middle, tab at the end\t');
SELECT encodeQuotedPrintable('line ends with space \r\nnext');

-- CRLF is kept as a hard line break, lone CR and LF are escaped.
SELECT encodeQuotedPrintable('a\r\nb\nc\rd');

-- Lines are at most 76 characters, with soft line breaks.
SELECT encodeQuotedPrintable(repeat('a', 100));
SELECT arrayMax(arrayMap(x -> length(x), splitByString('\r\n', encodeQuotedPrintable(repeat('é', 200)))));

-- Decoding.
SELECT decodeQuotedPrintable('Caf=C3=A9 =3D 3 =E2=82=AC');
SELECT decodeQuotedPrintable('lower=c3=a9case');
SELECT decodeQuotedPrintable('soft=\r\nbreak and soft=\nbreak');
SELECT decodeQuotedPrintable('bad escapes =ZZ =4 = stay, trailing equals is dropped=');

-- Round trip for arbitrary bytes.
SELECT countIf(decodeQuotedPrintable(encodeQuotedPrintable(s)) != s)
FROM (SELECT arrayStringConcat(arrayMap(i -> char(cityHash64(number, i) % 256), range(number % 300))) AS s FROM numbers(1000));

-- Works on columns, Nullable, and rejects FixedString.
SELECT encodeQuotedPrintable(toString(number) || '=') FROM numbers(3);
SELECT decodeQuotedPrintable(materialize(toNullable('=41=42')));
SELECT encodeQuotedPrintable(toFixedString('a', 1)); -- { serverError ILLEGAL_COLUMN }
