-- The HTML5 references &nGt; and &nLt; decode to two code points (6 bytes) from a
-- 5-byte reference, so a long run of them expands the output past the source length.
SELECT decodeHTMLComponent('&nGt;');
SELECT decodeHTMLComponent('&nLt;');
SELECT length(decodeHTMLComponent(repeat('&nGt;', 100000)));
SELECT length(decodeHTMLComponent(repeat('&nLt;', 100000)));
SELECT decodeHTMLComponent(repeat('&nGt;', 100000)) = repeat('≫⃒', 100000);
