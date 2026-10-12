-- Regression test for a heap buffer overflow in decodeHTMLComponent.
-- The output buffer was sized to the input length, but the entities `&nGt;` and `&nLt;` are 5 bytes
-- and decode to a 6-byte UTF-8 sequence, so a long run of them overflowed the output buffer.

-- A large run of the expanding entity must decode without overflowing (6 bytes per 5-byte entity).
SELECT length(decodeHTMLComponent(repeat('&nGt;', 200000)));
SELECT length(decodeHTMLComponent(repeat('&nLt;', 200000)));

-- Correctness of the expanding entities (U+226B/U+226A + combining U+20D2).
SELECT hex(decodeHTMLComponent('&nGt;'));
SELECT hex(decodeHTMLComponent('&nLt;'));

-- Ordinary entities still decode correctly.
SELECT decodeHTMLComponent('&lt;div&gt;Hello &amp; &quot;World&quot;&lt;/div&gt;');
