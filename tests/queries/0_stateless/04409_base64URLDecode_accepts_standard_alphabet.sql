-- base64URLDecode historically accepted the standard base64 alphabet ('+' and '/') in addition to the
-- URL-safe alphabet ('-' and '_'), because the old implementation only translated '-'/'_' before decoding
-- and left '+'/'/' untouched. Keep accepting both alphabets so previously valid inputs do not start failing.

SELECT base64URLDecode('+w==') = unhex('FB');
SELECT base64URLDecode('/w==') = unhex('FF');
SELECT tryBase64URLDecode('+w==') = unhex('FB');
SELECT tryBase64URLDecode('/w==') = unhex('FF');

-- The URL-safe alphabet (with or without padding) keeps working.
SELECT base64URLDecode('-w') = unhex('FB');
SELECT base64URLDecode('_w') = unhex('FF');

-- A mix of both alphabets in one input is accepted as well.
SELECT base64URLDecode('-_+/') = base64Decode('+/+/');
