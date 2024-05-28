SELECT base32Encode(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT base32Encode('hello, ', 'world!'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base32Encode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base32Decode(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT base32Decode('a', 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base32Decode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base32Decode('wrong string'); -- { serverError BAD_ARGUMENTS }

SELECT base32Encode('hello, world!');
SELECT base32Decode(base32Encode('hello, world!'));

SELECT base32Encode('');
SELECT base32Encode('a');
SELECT base32Encode('ab');
SELECT base32Encode('abc');
SELECT base32Encode('abcd');
SELECT base32Encode('abcde');
SELECT base32Encode('abcdea');
SELECT base32Encode('abcdeab');
SELECT base32Encode('abcdeabc');
SELECT base32Encode('abcdeabcd');
SELECT base32Encode('abcdeabcde');

SELECT base32Decode(base32Encode(''));
SELECT base32Decode(base32Encode('a'));
SELECT base32Decode(base32Encode('ab'));
SELECT base32Decode(base32Encode('abc'));
SELECT base32Decode(base32Encode('abcd'));
SELECT base32Decode(base32Encode('abcde'));
SELECT base32Decode(base32Encode('abcdea'));
SELECT base32Decode(base32Encode('abcdeab'));
SELECT base32Decode(base32Encode('abcdeabc'));
SELECT base32Decode(base32Encode('abcdeabcd'));
SELECT base32Decode(base32Encode('abcdeabcde'));