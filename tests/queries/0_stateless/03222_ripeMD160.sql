-- Tags: no-fasttest
-- Ouput can be verified using: https://emn178.github.io/online-tools/ripemd-160/

SELECT hex(RIPEMD160('The quick brown fox jumps over the lazy dog'));

SELECT hex(RIPEMD160('The quick brown fox jumps over the lazy cog'));

SELECT hex(RIPEMD160(''));

SELECT hex(RIPEMD160('CheREpaha1512'));

SELECT hex(RIPEMD160('A-very-long-string-that-should-be-hashed-using-RIPEMD160'));
